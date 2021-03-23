package common.CollectPrices.providers;

import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.events.MkvChainListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;

import common.CollectPrices.bus.PlatformWrapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class IonBusProvider extends GenericBusProvider
		implements MkvChainListener, MkvRecordListener, MkvPublishListener {

	private static Logger logger = LogManager.getLogger(IonBusProvider.class);

	public static String CHAIN_SEPARATOR = "\\.";

	private MkvPersistentChain chainSubscription;
	private PlatformWrapper platformWrapper;

	public IonBusProvider(String currency, String source, String instrument, String record, Set<String> fields) {
		// currency, source, instrumentChain, recordInstrument, fieldsSet
		super(currency, source, instrument, record, fields);
	}

	public IonBusProvider(PlatformWrapper platformWrapper, String currency, String source, String instrument,
			String record, Set<String> fields) {
		super(currency, source, instrument, record, fields);
		this.platformWrapper = platformWrapper;
	}

	public IonBusProvider(String chain, Set<String> fields) {
		super(chain, fields);
	}

	@PostConstruct public void init() {
		initChain();
	}


	public void initChain() {
		
		String chainName = this.toString();
		MkvSubscribeManager man = Mkv.getInstance().getSubscribeManager();
		logger.info("Suscribing to chain {} ", chainName);
		this.chainSubscription = man.persistentSubscribe(chainName, this, this);
	}
	
	//@PostConstruct
	public String initRecord() {
		String recordName = this.toString();
		Set<String> validFields = getValidFields(recordName, fields);
		if (validFields != null) {
			String[] fieldsAsArray = validFields.toArray(new String[validFields.size()]);
			MkvRecord mkvRecord = Mkv.getInstance().getPublishManager().getMkvRecord(recordName);
			try {
				mkvRecord.subscribe(fieldsAsArray, this);
			} catch (Exception e) {
				logger.error("initRecord: " + record + ": " + fields.toString() + " <- " + e);
			}
			return "";
		} else {
			logger.info("No exists record:" + recordName);
			return record;
		}
	}
	
	private Set<String> getValidFields(String record, Set<String> fields) {
		Set<String> validFields = null;
		try {
			MkvRecord mkvRecord = Mkv.getInstance().getPublishManager().getMkvRecord(record);
			if (mkvRecord!=null) {
				validFields = new HashSet<>();
				MkvType mkvType = Mkv.getInstance().getPublishManager().getMkvType(mkvRecord.getType());
				
				for (String field : fields) {
					if (mkvType.getFieldIndex(field) >= 0) {
						validFields.add(field);
					}
					
				}	
			}
		} catch (Exception e) {
			logger.error("No exists record: " + record + ": " + fields.toString() + " <- " + e);
		}
		
		return validFields;
	}
	

	@PreDestroy public void finish() {
		//		this.chainSubscription.close();
	}

	private static HashMap<String, Object> createDataFromMkvRecord(MkvRecord record, Set<String> fields) {
		HashMap<String, Object> data = new HashMap<String, Object>();
		MkvSupply supply = record.getSupply();
		MkvType type = record.getMkvType();
		int i = supply.firstIndex();
		while (i != -1) {
			String key;
			Object value = null;
			try {
				key = type.getFieldName(i);
				switch (type.getFieldType(i).intValue()) {
					case MkvFieldType.STR_code:
						value = supply.getString(i);
						break;
					case MkvFieldType.DATE_code:
					case MkvFieldType.TIME_code:
					case MkvFieldType.INT_code:
						value = supply.getInt(i);
						break;
					case MkvFieldType.REAL_code:
						value = supply.getDouble(i);
						if (value != null && ((Double) value).isNaN())
							value = null;
						break;
				}
				//Filtering to just published fields
				if (value != null && fields.contains(key))
					data.put(key, value);
				i = supply.nextIndex(i);
			} catch (MkvException e) {
				logger.error("BusProviderImpl.createDataFromMkvRecord",
						"Unable to read field from " + record.getName() + " record.");
				i = supply.nextIndex(i);
			}
		}
		if (!(data.containsKey("Id") || data.containsKey("ID"))) {
			// ANY.ANY.REUTERS.EUR10X13F_ICAP
			Object elem = record.toString().split(Pattern.quote("."))[3];
			data.put("Id",elem);
		}
		return data;
	}

	protected void updateObservers(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
		Map<String, Object> data = createDataFromMkvRecord(mkvRecord, this.fields);
		updateObservers(data);
	}

	@Override public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
		logger.trace("onPartialUpdate of mkvRecord:{} mkvSupply:{} isSnapshot:{}", mkvRecord, mkvSupply, isSnapshot);
		updateObservers(mkvRecord, mkvSupply, isSnapshot);
	}

	@Override public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
		logger.trace("onFullUpdate of mkvRecord:{} mkvSupply:{} isSnapshot:{}", mkvRecord, mkvSupply, isSnapshot);
		updateObservers(mkvRecord, mkvSupply, isSnapshot);
	}

	/**
	 * mkvPublishListeners
	 */
	@Override public void onPublish(MkvObject mkvObject, boolean b, boolean b1) {
		logger.trace("onPublish of {} {} {}", mkvObject, b, b1);

	}

	@Override public void onPublishIdle(String s, boolean b) {
		logger.trace("onSubscribe of {} {} ", s, b);

	}

	@Override public void onSubscribe(MkvObject mkvObject) {
		logger.trace("onSubscribe of {} ", mkvObject);
	}

	/**
	 * MkvChainListener
	 * Method that receives changes in chain , when new record is added ...etc
	 */
	@Override public void onSupply(MkvChain chain, String recordName, int pos, MkvChainAction action) {
		logger.trace("onSupply of {} {} {} {}", chain, recordName, pos, action);
	}
	
}
