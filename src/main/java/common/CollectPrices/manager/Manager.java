
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
	
	@Value("${mkv.pubSub.sender.project}") String project;
	@Value("${mkv.pubSub.sender.type:Topic}") String topic;
	@Value("${mkv.pubSub.sender.credentialsFile}") String credentialsFile;
	@Value("${Manager.timeWindowLength:300000}") // 5 min
	private long timeWindowLength;
	
	@Value("#{'${mkv.providers}'.split(',')}") List<String> providers;

	@Value("${desk}") String desk;
	
	private ShowVwapThread showVwapThread;
	
	protected static Gson GSON = new GsonBuilder().setPrettyPrinting().serializeSpecialFloatingPointValues().create();
	
	private Logger logger = LogManager.getLogger(Manager.class);
	
	private PubSubType pubSubType;
	
	private Map<String, BusRetransmitter> busRetransmitterMap;
	private Map<String, IonBusProvider> ionBusProvidersMap;
	private Map<String, QueueConfiguration> configurationsMap;
	private Map<String, Map<String, TreeMap<Long, ElemtPriceQty>>> mapProviderIsinVwap;
	
	protected static final String BID_PRICE_FIELD = "BID_PRICE";
	protected static final String ASK_PRICE_FIELD = "ASK_PRICE";
	protected static final String BID_SIZE_FIELD = "BID_SIZE";
	protected static final String ASK_SIZE_FIELD = "ASK_SIZE";

	
	@PostConstruct private void init() {
		configurationsMap = new HashMap<>();
		ionBusProvidersMap = new HashMap<>();
		busRetransmitterMap = new HashMap<>();
		mapProviderIsinVwap = new ConcurrentHashMap<>();
		
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
				
				// 2- Save price and Qty
				Map<String, TreeMap<Long, ElemtPriceQty>> mapIsinAskBidVwap = mapProviderIsinVwap.get(this.nameProvider);
				if (mapIsinAskBidVwap == null) {
					mapIsinAskBidVwap = new HashMap<>();
					
					saveTreeMapAskBid(fields, timestamp, idBus, mapIsinAskBidVwap);
					
					mapProviderIsinVwap.put(this.nameProvider, mapIsinAskBidVwap);

				} else {
										
					TreeMap<Long, ElemtPriceQty> mapIsinAskBidVwapAux = mapIsinAskBidVwap.get(idBus+"_Ask");
					if (mapIsinAskBidVwapAux==null) {
						
						saveTreeMapAskBid(fields, timestamp, idBus, mapIsinAskBidVwap);
						
					} else {
						// Returns the greatest key less than or equal to the given key,or null if there is no such key
						Long cutoffTimeAsk = mapIsinAskBidVwap.get(idBus + "_Ask").floorKey(timestamp - timeWindowLength);
						if (cutoffTimeAsk != null) {
							Long firstTime = mapIsinAskBidVwap.get(idBus + "_Ask").firstKey();
							while (firstTime != cutoffTimeAsk) {
								mapIsinAskBidVwap.get(idBus + "_Ask").remove(firstTime);
								firstTime = mapIsinAskBidVwap.get(idBus + "_Ask").firstKey();
							}
						}
						
						Long cutoffTimeBid = mapIsinAskBidVwap.get(idBus+"_Bid").floorKey(timestamp - timeWindowLength);
						if (cutoffTimeBid != null) {
							Long firstTime = mapIsinAskBidVwap.get(idBus + "_Bid").firstKey();
							while (firstTime != cutoffTimeBid) {
								mapIsinAskBidVwap.get(idBus + "_Bid").remove(firstTime);
								firstTime = mapIsinAskBidVwap.get(idBus + "_Bid").firstKey();
							}
						}
						
						mapIsinAskBidVwap.get(idBus+"_Ask").put(timestamp, new ElemtPriceQty((double) fields.get("Ask0"), (int) fields.get("AskSize0")));
						mapIsinAskBidVwap.get(idBus+"_Bid").put(timestamp, new ElemtPriceQty((double) fields.get("Bid0"), (int) fields.get("BidSize0")));
					}
				}
			}
		}

		private void saveTreeMapAskBid(Map<String, Object> fields, long timestamp, String idBus,
				Map<String, TreeMap<Long, ElemtPriceQty>> mapIsinAskBidVwap) {
			TreeMap<Long, ElemtPriceQty> mapTimeAsk = new TreeMap<Long, ElemtPriceQty>();
			mapTimeAsk.put(timestamp, new ElemtPriceQty((double) fields.get("Ask0"), (int) fields.get("AskSize0")));

			TreeMap<Long, ElemtPriceQty> mapTimeBid = new TreeMap<Long, ElemtPriceQty>();
			mapTimeBid.put(timestamp, new ElemtPriceQty((double) fields.get("Bid0"), (int) fields.get("BidSize0")));
			
			mapIsinAskBidVwap.put(idBus+"_Ask", mapTimeAsk);
			mapIsinAskBidVwap.put(idBus+"_Bid", mapTimeBid);
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
	
	private class ShowVwapThread implements Runnable {

		private boolean start = false;

		private long SLEEP_CYCLE_MS = 600000;//every 600 seconds

		public ShowVwapThread() {
			this.start = true;
		}
		@Override
		public void run() {
			while (this.start) {
				
				logger.info("********************* INIT VWAP ****************************");
				// private Map<String, Map<String, TreeMap<Long, ElemtPriceQty>>> mapProviderIsinVwap;
				if (mapProviderIsinVwap!=null) {
					// for providers
					for (Entry<String, Map<String, TreeMap<Long, ElemtPriceQty>>> entrySet : mapProviderIsinVwap.entrySet()) {
						String provider = entrySet.getKey();
						logger.info("\tProvider --> {} :", provider);
						
						Map<String, TreeMap<Long, ElemtPriceQty>> mapIsinVwap = entrySet.getValue();
						// for isins of provider
						for (Entry<String, TreeMap<Long, ElemtPriceQty>> entrySetMap : mapIsinVwap.entrySet()) {
							String isinBidAsk = entrySetMap.getKey();
							TreeMap<Long, ElemtPriceQty> vwapIsin = entrySetMap.getValue();
							logger.info("\t\tIsin{} :", isinBidAsk);
							Double sumQtyPrice = null;
							Integer acumQty = null;
							// for ask and bid of isin
							for (Entry<Long, ElemtPriceQty> entrySetAskBidMap : vwapIsin.entrySet()) {
								Long timeStamp = entrySetAskBidMap.getKey();
								ElemtPriceQty elem = entrySetAskBidMap.getValue();
								logger.debug("\t\t\t time{} price{} qty{}.", timeStamp, elem.getPrice(), elem.getQty());
								// Σ ((qty * precio)) / volumen total.
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
						}
					}
				}
				
				logger.info("********************* END VWAP ****************************");
				
				// sleep
				try {
					Thread.sleep(SLEEP_CYCLE_MS);
				} catch (InterruptedException e) {
					logger.error("ShowVwapThread --> run(): error sleeping in ShowVwapThread ", e);
				}
			}
			
		}
		
	}
	
}