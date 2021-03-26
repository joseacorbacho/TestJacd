
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

/**
 * Clase Manager principal encarga de la gestion de:
 * 	- Subcribirse al bus de ION(a la informacion definidaen los properties) para recoger la info de precios y qtys.
 *  - Guardar la informacion en una estructura de datos.
 *  - Calcular Vwap para cad instrumento en tiempo Real.
 *  
 * @author Jose
 *
 */

public class Manager {
	
	// Para leer los ficheros properties
	@Autowired PropertyPlaceholderExposer properties;
	
	// Definir una ventana de tiempo, para guardar los cambios de precios
	@Value("${Manager.timeWindowLength:300000}") // 5 min
	private long timeWindowLength;

	// Variable donde guardaremos los providers de los que queremos leer
	@Value("#{'${mkv.providers}'.split(',')}") 
	private List<String> providers;
	
	private static Logger logger = LogManager.getLogger(Manager.class);
	
	private static final String SEPARATOR = "_";
	
	private static final String ASK0 = "Ask0";
	private static final String _ASK = "_Ask";
	private static final String ASK_SIZE0 = "AskSize0";
	
	private static final String BID0 = "Bid0";
	private static final String _BID = "_Bid";
	private static final String BID_SIZE0 = "BidSize0";
	
	// Mapa donde se guardaran las conexiones al bus para cada provider
	private Map<String, BusRetransmitter> busRetransmitterMap;
	// Mapa donde guardaremos los Precios-Qty(Bid-Ask) de cada instrumento en el tiempo para cada provider configurado 
	private Map<String, Map<String, TreeMap<Long, ElemtPriceQty>>> mapProviderIsinVwap;

	
	@PostConstruct private void init() {
		busRetransmitterMap = new HashMap<>();
		mapProviderIsinVwap = new ConcurrentHashMap<>();
		
		//Recorreremos los providers definidos en el properties, para subcribirnos al bus a sus configuraciones
		int i = 0;
		for (String provider : providers) {
 			i++;
			logger.trace("*****************************************************************************************");
			try {
				logger.trace("init: {}.0 Creating bus subscriber to {}", i, provider);
				String type = getPropertyMkv(provider, "type");
				String source = getPropertyMkv(provider, "source");
				String instrument = getPropertyMkv(provider, "instrument");
				String currency = getPropertyMkv(provider, "currency");
				String record = getPropertyMkv(provider, "record");
				Set<String> fields = getPropertySet(provider, "fields", ",");
				
				IonBusProvider ionBusProvider = null;
				
				logger.trace("init: {}.1- Creating subcription with record: {}.", i, record);
				ionBusProvider = new IonBusProvider(currency, source, instrument, record, fields);
				try {
					// Inicializamos las subcripcion a las cadenas, para que nos lleguen los eventos de cambio de Precios-Qty
					ionBusProvider.initChain();
				} catch (NullPointerException ex) {
					logger.error("NullPointerException init initChain ionBusProvider! on {} {} {} {}", currency, source, instrument, record);
				}
				
				BusRetransmitter busRetransmitter = busRetransmitterMap.get(provider);
				if (busRetransmitter == null) {
					busRetransmitter = new BusRetransmitter(provider, source);
					busRetransmitterMap.put(provider, busRetransmitter);
				}
				ionBusProvider.addObserver(busRetransmitter);
				
				logger.trace("init: {}.2- Created bus subscriber for providers {}.", i, provider);
				
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
	
	public class BusRetransmitter implements BusObserver {

		private String nameProvider, source;
		private Map<String, Map<String, Object>> lastMessageSent;

		public BusRetransmitter(String name, String source) {
			this.nameProvider = name;
			this.source = source;
			this.lastMessageSent = new HashMap<>();
		}

		@Override
		public synchronized void update(Map<String, Object> fields) {
			// Aqui llegaran los updates con los cambios de precios-qty, y trataremos la informacion para el calculo en tiempo Real de VWap
			String key = "Id";

			long timestamp = System.currentTimeMillis();

			Map<String, Object> cleanFields = fields;
			if (cleanFields.containsKey(key) || cleanFields.containsKey(key.toUpperCase())) {
				String idBus = (String) cleanFields.getOrDefault(key, "");
				if (idBus.isEmpty()) {
					idBus = (String) cleanFields.getOrDefault(key.toUpperCase(), "");
				}
				Map<String, Object> completeFields = completeWithCache(idBus, cleanFields);
				if (isAlreadySent(idBus, completeFields)) {
					return;
				}
				
				// 1- Save price and Qty(for 1 isin of 1 provider)
				// private Map<String, Map<String, TreeMap<Long, ElemtPriceQty>>> mapProviderIsinVwap;
				Map<String, TreeMap<Long, ElemtPriceQty>> mapIsinAskBidVwap = mapProviderIsinVwap.get(this.nameProvider);
				if (mapIsinAskBidVwap == null) {
					// Guardamos los primeros valores para ese provider
					mapIsinAskBidVwap = new HashMap<>();
					
					saveTreeMapAskBid(fields, timestamp, idBus, mapIsinAskBidVwap);
					
					mapProviderIsinVwap.put(this.nameProvider, mapIsinAskBidVwap);

				} else {
										
					TreeMap<Long, ElemtPriceQty> mapIsinAskBidVwapAux = mapIsinAskBidVwap.get(idBus+_ASK);
					if (mapIsinAskBidVwapAux==null) {
						// Guardamos los primeros valores para ese isin-provider
						saveTreeMapAskBid(fields, timestamp, idBus, mapIsinAskBidVwap);
						
					} else {
						// Ya tenemos valores guardados para ese isin-provider, añadimos mas informacion
						
						// Mantener en la estructura de datos, la informacion de la ventana de tiempo configurada
						// floorKey-- > Returns the greatest key less than or equal to the given key,or null if there is no such key
						Long cutoffTimeAsk = mapIsinAskBidVwap.get(idBus + _ASK).floorKey(timestamp - timeWindowLength);
						if (cutoffTimeAsk != null) {
							Long firstTime = mapIsinAskBidVwap.get(idBus + _ASK).firstKey();
							while (firstTime != cutoffTimeAsk) {
								mapIsinAskBidVwap.get(idBus + _ASK).remove(firstTime);
								firstTime = mapIsinAskBidVwap.get(idBus + _ASK).firstKey();
							}
						}
						
						// floorKey-- > Returns the greatest key less than or equal to the given key,or null if there is no such key
						Long cutoffTimeBid = mapIsinAskBidVwap.get(idBus+_BID).floorKey(timestamp - timeWindowLength);
						if (cutoffTimeBid != null) {
							Long firstTime = mapIsinAskBidVwap.get(idBus + _BID).firstKey();
							while (firstTime != cutoffTimeBid) {
								mapIsinAskBidVwap.get(idBus + _BID).remove(firstTime);
								firstTime = mapIsinAskBidVwap.get(idBus + _BID).firstKey();
							}
						}
						
						// Despues de ajustar los datos a la ventana de tiempo configurada, guardamos la info
						mapIsinAskBidVwap.get(idBus + _ASK).put(timestamp, new ElemtPriceQty((double) fields.get(ASK0), (int) fields.get(ASK_SIZE0)));
						mapIsinAskBidVwap.get(idBus + _BID).put(timestamp, new ElemtPriceQty((double) fields.get(BID0), (int) fields.get(BID_SIZE0)));
					}
				}
				
				// Despues de guardar la info, hacemos el calculo(en tiempo real) de vwap para el provider, isin y bid y ask.
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

	private void calculateVwapIsin(String provider, String isin, String askBid, TreeMap<Long, ElemtPriceQty> treeMapAskBid) {
		logger.trace("********************* INIT VWAP FOR PROVIDER {} AND ISIN {} ****************************", provider, isin);
		
		logger.trace("\t\tIsin{} ({}):", isin, askBid);
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
				logger.trace("\t\t\t VWAP {} :", (sumQtyPrice / acumQty));
			} else {
				logger.error("\\t\\t\\t Could not calculate VWAP.");
			}
		} else {
			logger.error("\\t\\t\\t Could not calculate VWAP.");
		}
		
		
		logger.trace("********************* END VWAP FOR PROVIDER {} AND ISIN {} ****************************", provider, isin);
	}
	
	private void calculateVwap() {
		logger.trace("********************* INIT VWAP ALL PROVIDERS AND ALL ISINS ****************************");
		// private Map<String, Map<String, TreeMap<Long, ElemtPriceQty>>> mapProviderIsinVwap;
		if (mapProviderIsinVwap!=null) {
			// for providers
			for (Entry<String, Map<String, TreeMap<Long, ElemtPriceQty>>> entrySet : mapProviderIsinVwap.entrySet()) {
				String provider = entrySet.getKey();
				logger.trace("\tProvider --> {} :", provider);
				
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
		
		logger.trace("********************* END VWAP ALL PROVIDERS AND ALL ISINS ****************************");
	}
}