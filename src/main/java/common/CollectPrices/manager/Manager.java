
package common.CollectPrices.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import common.CollectPrices.providers.BusObserver;
import common.CollectPrices.providers.IonBusProvider;
import common.CollectPrices.publisher.PubSubConfiguration;
import common.CollectPrices.publisher.PubSubType;
import common.CollectPrices.publisher.QueueConfiguration;
import common.CollectPrices.publisher.QueuePublisher;
import common.CollectPrices.spring.PropertyPlaceholderExposer;
import common.CollectPrices.tick.GenericTick;
import common.CollectPrices.tick.SourceTick;


public class Manager implements ManagerInterface {
	
	@Autowired PropertyPlaceholderExposer properties;

	@Autowired protected QueuePublisher queuePublisher;
	
	@Value("${mkv.pubSub.sender.project}") 
	private String project;
	@Value("${mkv.pubSub.sender.type:Topic}") 
	private String topic;
	@Value("${mkv.pubSub.sender.credentialsFile}") 
	private String credentialsFile;
	@Value("${Manager.timeWindowLength:300000}") // 5 min
	private long timeWindowLength;
	@Value("${Manager.sleepCycleMs:600000}") // 10 min
	private long SLEEP_CYCLE_MS;

	@Value("#{'${mkv.providers}'.split(',')}") 
	private List<String> providers;

	@Value("${desk}") 
	private String desk;
	
	private static final String BID_PRICE_FIELD = "BID_PRICE";
	private static final String ASK_PRICE_FIELD = "ASK_PRICE";
	private static final String BID_SIZE_FIELD = "BID_SIZE";
	private static final String ASK_SIZE_FIELD = "ASK_SIZE";
	
	private static final String ASK0 = "Ask0";
	private static final String _ASK = "_Ask";
	private static final String ASK_SIZE0 = "AskSize0";
	
	private static final String BID0 = "Bid0";
	private static final String _BID = "_Bid";
	private static final String BID_SIZE0 = "BidSize0";
	
	private static final String SEPARATOR = "_";
	
	protected static Gson GSON = new GsonBuilder().setPrettyPrinting().serializeSpecialFloatingPointValues().create();
	
	private Logger logger = LogManager.getLogger(Manager.class);
	
	private ShowVwapThread showVwapThread;
	
	private PubSubType pubSubType;
	
	private Map<String, BusRetransmitter> busRetransmitterMap;
	private Map<String, IonBusProvider> ionBusProvidersMap;
	private Map<String, QueueConfiguration> configurationsMap;
	private Map<String, Map<String, TreeMap<Long, ElemtPriceQty>>> mapProviderIsinVwap;

	
	@PostConstruct private void init() {
		configurationsMap = new HashMap<>();
		ionBusProvidersMap = new HashMap<>();
		busRetransmitterMap = new HashMap<>();
		mapProviderIsinVwap = new ConcurrentHashMap<>();
		
		// Thread for vwap calculation
		this.showVwapThread = new ShowVwapThread();
		
		pubSubType = PubSubType.get(topic);
		
		int i = 0;
		for (String provider : providers) {
 			i++;
			logger.info("*****************************************************************************************");
			try {
				logger.info("init: {}.0 Creating bus subscriber to {}", i, provider);
				String type = getPropertyMkv(provider, "type");
				String source = getPropertyMkv(provider, "source");
				String instrument = getPropertyMkv(provider, "instrument");
				String currency = getPropertyMkv(provider, "currency");
				String record = getPropertyMkv(provider, "record");
				Set<String> fields = getPropertySet(provider, "fields", ",");
				
				IonBusProvider ionBusProvider = null;
				
				logger.info("init: {}.1- Creating with record no empty: {}.", i, record);
				ionBusProvider = new IonBusProvider(currency, source, instrument, record, fields);
				try {
					ionBusProvider.initChain();
				} catch (NullPointerException ex) {
					logger.error("NullPointerException init initChain ionBusProvider! on {} {} {} {}", currency, source, instrument, record);
				}
				prepareSettings(provider, source, type, ionBusProvider);
				logger.info("init: {}.2- Created bus subscriber for providers {}.", i, provider);
				
			} catch (Exception e) {
				logger.error("Error creating provider to {} ", provider, e);
			}
		}	
	}
	
	private String getPropertyMkv(String provider, String property) {
		String propertyStr = String.format("mkv.%s.%s", provider, property);
		logger.debug("getting property {}", propertyStr);
		String output = properties.get(propertyStr);
		return output;
	}
	
	private Set<String> getPropertySet(String provider, String property, String separator) {
		String propertiesStr = getPropertyMkv(provider, property);
		if (propertiesStr == null) {
			logger.error("Cant find {} in properties to get list ", property);
			return null;
		}
		if (!propertiesStr.isEmpty()) {
			List<String> result = Arrays.asList(propertiesStr.split(separator));
			return new HashSet<>(result);
		}
		return new HashSet<>();
	}
	
	private void prepareSettings(String provider, String source, String type, IonBusProvider ionBusProvider)
			throws Exception {
		
		String typeMessage = getPropertyMkv(provider, "type");

		QueueConfiguration pubSubConfiguration = obtainQueueConfiguration(provider, typeMessage);
		
		BusRetransmitter busRetransmitter = busRetransmitterMap.get(provider);
		if (busRetransmitter == null) {
			busRetransmitter = new BusRetransmitter(provider, source, type, pubSubConfiguration);
			busRetransmitterMap.put(provider, busRetransmitter);
		}
		ionBusProvider.addObserver(busRetransmitter);
		ionBusProvidersMap.put(provider + ionBusProvider.getRecord(), ionBusProvider);
		
	}
	
	public QueueConfiguration obtainQueueConfiguration(String provider, String typeMessage)  throws Exception {
		
		String topic = getPropertyMkv(provider, "topic");
		QueueConfiguration pubSubConfiguration = getQueueConfiguration(topic);
		if (pubSubConfiguration == null) {
			pubSubConfiguration = createQueueConfiguration(topic, typeMessage);
			configurationsMap.put(topic,pubSubConfiguration);
		}
		return pubSubConfiguration;
	}
	
	public QueueConfiguration getQueueConfiguration(String topic) {
		return configurationsMap.get(topic);
	}
	
	private QueueConfiguration createQueueConfiguration(String topic, String typeMessage) throws Exception {

		if (queuePublisher instanceof common.CollectPrices.publisher.PubSubMessagePublisher) {
			PubSubConfiguration pubSubConfiguration = new PubSubConfiguration(credentialsFile, topic, "subscriptionId", project, pubSubType);
			return pubSubConfiguration;
		} else {
			logger.error("Cant get configuration of class {}", queuePublisher);
			throw new Exception("Cant get configuration of class " + queuePublisher);
		}
	}


	@Override
	public Integer sendToGoogle(String idBus, long timestamp, String type, String source,
			QueueConfiguration queueConfiguration, AtomicInteger messagesSent, Map<String, Object> completeFields) {
		
		GenericTick messageComplete = wrapFields(type, source, completeFields, timestamp);
		if (messageComplete == null) {
			return 0;
		}

		String receivedJson = "";
		try {
			receivedJson = GSON.toJson(messageComplete);
		} catch (Exception e) {
			logger.error("Error converting {} {} to json", idBus, messageComplete, e);
			return 0;
		}
		
		logger.debug("full update partial received ,sending{}", receivedJson);
		try {

			queuePublisher.send(receivedJson, queueConfiguration, timestamp);

		} catch (Exception e) {
			logger.error("Error sending {} ", receivedJson, e);
			return 0;
		}
		
		if (messagesSent!=null) {
			messagesSent.incrementAndGet();
		}
		return 1;
	}
	
	private GenericTick wrapFields(String type, String source, Map<String, Object> fields, long time) {
		GenericTick tick = null;

		tick = new SourceTick(desk, source, type, time);

		tick = getLegacyOrderBookFields(type, source, tick, fields);
		if (tick.getInstrumentId() == null || tick.getInstrumentId().equalsIgnoreCase("")) {
			tick = null;
		}

		return tick;
	}
	
	private GenericTick getLegacyOrderBookFields(String topic, String source, GenericTick tick, Map<String, Object> fields) {
		
		tick.setInstrumentId(fields.get("Id").toString());
		tick.setType("ORDER_BOOK");
		boolean status = false;
		for (String keyField : fields.keySet()) {
			if (keyField.matches("Ask[0-9]*$")) {
				//Ask level
				double value = (double) fields.get(keyField);
				String level = keyField.replaceFirst(".*?(\\d+).*", "$1");
				String newKey = ASK_PRICE_FIELD + level;
				tick.addValue(newKey, value);
				if (Integer.valueOf(level) == 0 && value != 0) {
					status = true;
				}
			}
			if (keyField.equals(ASK_PRICE_FIELD) || keyField.equals("O_Ask")) {
				//Ask level
				double value = (double) fields.get(keyField);
				String level = "0";
				String newKey = ASK_PRICE_FIELD + level;
				tick.addValue(newKey, value);
				if (Integer.valueOf(level) == 0 && value != 0) {
					status = true;
				}
			}
			if (keyField.matches("Bid[0-9]*$")) {
				//Bid level
				double value = (double) fields.get(keyField);
				String level = keyField.replaceFirst(".*?(\\d+).*", "$1");
				String newKey = BID_PRICE_FIELD + level;
				tick.addValue(newKey, value);
				if (Integer.valueOf(level) == 0 && value != 0) {
					status = true;
				}
			}
			if (keyField.equals(BID_PRICE_FIELD) || keyField.equals("O_Bid")) {
				//Bid level
				double value = (double) fields.get(keyField);
				String level = "0";
				String newKey = BID_PRICE_FIELD + level;
				tick.addValue(newKey, value);
				if (Integer.valueOf(level) == 0 && value != 0) {
					status = true;
				}
			}
			if (keyField.matches("AskSize[0-9]*$")) {
				//AskSize level
				double value = (double) fields.get(keyField);
				String level = keyField.replaceFirst(".*?(\\d+).*", "$1");
				String newKey = ASK_SIZE_FIELD + level;
				tick.addValue(newKey, value);
				if (Integer.valueOf(level) == 0 && value != 0) {
					status = true;
				}
			}
			if (keyField.equals(ASK_SIZE_FIELD) || keyField.equals("O_AskQty")) {
				//AskSize level
				double value = (double) fields.get(keyField);
				String level = "0";
				String newKey = ASK_SIZE_FIELD + level;
				tick.addValue(newKey, value);
				if (Integer.valueOf(level) == 0 && value != 0) {
					status = true;
				}
			}
			if (keyField.matches("BidSize[0-9]*$")) {
				//BidSize level
				double value = (double) fields.get(keyField);
				String level = keyField.replaceFirst(".*?(\\d+).*", "$1");
				String newKey = BID_SIZE_FIELD + level;
				tick.addValue(newKey, value);
				if (Integer.valueOf(level) == 0 && value != 0) {
					status = true;
				}

			}
			if (keyField.equals(BID_SIZE_FIELD) || keyField.equals("O_BidQty")) {
				//BidSize level
				double value = (double) fields.get(keyField);
				String level = "0";
				String newKey = BID_SIZE_FIELD + level;
				tick.addValue(newKey, value);
				if (Integer.valueOf(level) == 0 && value != 0) {
					status = true;
				}
			}

		}
		
		if (!status) {
			tick.setInvalidStatus();
		}
		return tick;
	}
	
	public class BusRetransmitter implements BusObserver {

		private QueueConfiguration queueConfiguration;
		private String nameProvider, type, source;
		protected AtomicInteger messagesSent = new AtomicInteger(0);
		private Map<String, Map<String, Object>> lastMessageSent;

		public BusRetransmitter(String name, String source, String type, QueueConfiguration queueConfiguration) {
			this.nameProvider = name;
			this.queueConfiguration = queueConfiguration;
			this.type = type;
			this.source = source;
			this.lastMessageSent = new HashMap<>();
		}

		public int getMessagesSent() {
			return queuePublisher.getMessagesSent(this.queueConfiguration);
		}

		public int getMessagesNotSent() {
			return queuePublisher.getErrorCounter(this.queueConfiguration);
		}

		@Override
		public synchronized void update(Map<String, Object> fields) {
			String smsNoSent;
			String key = "Id";

			long timestamp = System.currentTimeMillis();

			Map<String, Object> cleanFields = fields;
			if (cleanFields.containsKey(key) || cleanFields.containsKey(key.toUpperCase())) {
				String idBus = (String) cleanFields.getOrDefault(key, "");
				if (idBus.isEmpty()) {
					// For Pricer updates
					idBus = (String) cleanFields.getOrDefault(key.toUpperCase(), "");
				}
				Map<String, Object> completeFields = completeWithCache(idBus, cleanFields);
				if (isAlreadySent(idBus, completeFields)) {
					return;
				}

				// 1- Send to Google
				if (sendToGoogle(idBus, timestamp, this.type, this.source, queueConfiguration, messagesSent,
						completeFields) != 1) {
					smsNoSent = "idBus(" + idBus + "), timestampSent(" + timestamp + "), type(" + type + "), source("
							+ source + "), QueueConfiguration(" + queueConfiguration + "), AtomicInteger("
							+ messagesSent + "), completeFields Size(" + completeFields.size() + ").";
					logger.debug("The message could not be sent: " + smsNoSent);
				}
				
				// 2- Save price and Qty(for 1 isin of 1 provider)
				// private Map<String, Map<String, TreeMap<Long, ElemtPriceQty>>> mapProviderIsinVwap;
				Map<String, TreeMap<Long, ElemtPriceQty>> mapIsinAskBidVwap = mapProviderIsinVwap.get(this.nameProvider);
				if (mapIsinAskBidVwap == null) {
					mapIsinAskBidVwap = new HashMap<>();
					
					saveTreeMapAskBid(fields, timestamp, idBus, mapIsinAskBidVwap);
					
					mapProviderIsinVwap.put(this.nameProvider, mapIsinAskBidVwap);

				} else {
										
					TreeMap<Long, ElemtPriceQty> mapIsinAskBidVwapAux = mapIsinAskBidVwap.get(idBus+_ASK);
					if (mapIsinAskBidVwapAux==null) {
						
						saveTreeMapAskBid(fields, timestamp, idBus, mapIsinAskBidVwap);
						
					} else {
						// Returns the greatest key less than or equal to the given key,or null if there is no such key
						Long cutoffTimeAsk = mapIsinAskBidVwap.get(idBus + _ASK).floorKey(timestamp - timeWindowLength);
						if (cutoffTimeAsk != null) {
							Long firstTime = mapIsinAskBidVwap.get(idBus + _ASK).firstKey();
							while (firstTime != cutoffTimeAsk) {
								mapIsinAskBidVwap.get(idBus + _ASK).remove(firstTime);
								firstTime = mapIsinAskBidVwap.get(idBus + _ASK).firstKey();
							}
						}
						
						Long cutoffTimeBid = mapIsinAskBidVwap.get(idBus+_BID).floorKey(timestamp - timeWindowLength);
						if (cutoffTimeBid != null) {
							Long firstTime = mapIsinAskBidVwap.get(idBus + _BID).firstKey();
							while (firstTime != cutoffTimeBid) {
								mapIsinAskBidVwap.get(idBus + _BID).remove(firstTime);
								firstTime = mapIsinAskBidVwap.get(idBus + _BID).firstKey();
							}
						}
						
						mapIsinAskBidVwap.get(idBus + _ASK).put(timestamp, new ElemtPriceQty((double) fields.get(ASK0), (int) fields.get(ASK_SIZE0)));
						mapIsinAskBidVwap.get(idBus + _BID).put(timestamp, new ElemtPriceQty((double) fields.get(BID0), (int) fields.get(BID_SIZE0)));
					}
				}
				
				// Calculete vwap for each change in the bus(provider and isin)				
				calculateVwapIsin(this.nameProvider, idBus, _ASK, mapProviderIsinVwap.get(this.nameProvider).get(idBus + _ASK));
				calculateVwapIsin(this.nameProvider, idBus, _BID, mapProviderIsinVwap.get(this.nameProvider).get(idBus + _BID));
			}
		}

		private void saveTreeMapAskBid(Map<String, Object> fields, long timestamp, String idBus,
				Map<String, TreeMap<Long, ElemtPriceQty>> mapIsinAskBidVwap) {
			TreeMap<Long, ElemtPriceQty> mapTimeAsk = new TreeMap<Long, ElemtPriceQty>();
			mapTimeAsk.put(timestamp, new ElemtPriceQty((double) fields.get(ASK0), (int) fields.get(ASK_SIZE0)));

			TreeMap<Long, ElemtPriceQty> mapTimeBid = new TreeMap<Long, ElemtPriceQty>();
			mapTimeBid.put(timestamp, new ElemtPriceQty((double) fields.get(BID0), (int) fields.get(BID_SIZE0)));
			
			mapIsinAskBidVwap.put(idBus + _ASK, mapTimeAsk);
			mapIsinAskBidVwap.put(idBus + _BID, mapTimeBid);
		}

		@Override public void fullUpdate(Map<String, Object> fields) {

		}

		/**
		 * Check that messages are not sent duplicated
		 *
		 * @param id
		 * @param fields
		 * @return
		 */
		private boolean isAlreadySent(String id, Map<String, Object> fields) {
			if (!this.lastMessageSent.containsKey(id)) {
				this.lastMessageSent.put(id, fields);
				return false;
			}

			Map<String, Object> cacheMessage = this.lastMessageSent.get(id);
			if (fields.equals(cacheMessage)) {
				return true;
			} else {
				this.lastMessageSent.put(id, fields);
				return false;
			}

		}

		private Map<String, Object> completeWithCache(String id, Map<String, Object> fields) {
			Map<String, Object> output = new HashMap<>(fields);
			Map<String, Object> cacheMessage = this.lastMessageSent.get(id);
			if (cacheMessage != null) {
				for (Map.Entry<String, Object> entry : cacheMessage.entrySet()) {
					String key = entry.getKey();
					Object value = entry.getValue();
					if (!output.containsKey(key)) {
						output.put(key, value);
					}
				}
			}
			return output;
		}
	}
	
	private class ShowVwapThread implements Runnable {

		private boolean start = false;

		public ShowVwapThread() {
			this.start = true;
		}
		@Override
		public void run() {
			while (this.start) {
				
				calculateVwap();
				
				// sleep
				try {
					Thread.sleep(SLEEP_CYCLE_MS);
				} catch (InterruptedException e) {
					logger.error("Error sleeping in calculateVwap ", e);
				}
			}	
		}
	}
	
	private void calculateVwap() {
		logger.info("********************* INIT VWAP ALL PROVIDERS AND ALL ISINS ****************************");
		// private Map<String, Map<String, TreeMap<Long, ElemtPriceQty>>> mapProviderIsinVwap;
		if (mapProviderIsinVwap!=null) {
			// for providers
			for (Entry<String, Map<String, TreeMap<Long, ElemtPriceQty>>> entrySet : mapProviderIsinVwap.entrySet()) {
				String provider = entrySet.getKey();
				logger.info("\tProvider --> {} :", provider);
				
				Map<String, TreeMap<Long, ElemtPriceQty>> mapIsinAskBidVwap = entrySet.getValue();
				
				// for isins of provider
				for (Entry<String, TreeMap<Long, ElemtPriceQty>> entrySetMap : mapIsinAskBidVwap.entrySet()) {
					String isinBidAsk = entrySetMap.getKey();
					TreeMap<Long, ElemtPriceQty> vwapIsin = entrySetMap.getValue();
					
					String[] elems = isinBidAsk.split(SEPARATOR);
					calculateVwapIsin(provider, elems[0], SEPARATOR + elems[1], vwapIsin);
				}
			}
		}
		
		logger.info("********************* END VWAP ALL PROVIDERS AND ALL ISINS ****************************");
	}

	private void calculateVwapIsin(String provider, String isin, String askBid, TreeMap<Long, ElemtPriceQty> treeMapAskBid) {
		logger.info("********************* INIT VWAP FOR PROVIDER {} AND ISIN {} ****************************", provider, isin);
		
		logger.info("\t\tIsin{} ({}):", isin, askBid);
		if (treeMapAskBid !=null) {
			Double sumQtyPrice = null;
			Integer acumQty = null;
			// for ask and bid of isin
			for (Entry<Long, ElemtPriceQty> entrySetAskBidMap : treeMapAskBid.entrySet()) {
				Long timeStamp = entrySetAskBidMap.getKey();
				ElemtPriceQty elem = entrySetAskBidMap.getValue();
				logger.debug("\t\t\t time{} price{} qty{}.", timeStamp, elem.getPrice(), elem.getQty());
				// SUM ((qty * precio)) / volumen total.
				if (sumQtyPrice==null) {
					sumQtyPrice = elem.getPrice() * elem.getQty();
					acumQty = elem.getQty();
				} else {
					sumQtyPrice = sumQtyPrice + (elem.getPrice() * elem.getQty());
					acumQty = acumQty + elem.getQty();
				}
			}
			if (sumQtyPrice!=null && acumQty!=null) {
				logger.info("\t\t\t VWAP {} :", (sumQtyPrice / acumQty));
			} else {
				logger.error("\\t\\t\\t Could not calculate VWAP.");
			}
		} else {
			logger.error("\\t\\t\\t Could not calculate VWAP.");
		}
		
		
		logger.info("********************* END VWAP FOR PROVIDER {} AND ISIN {} ****************************", provider, isin);
		
	}
}