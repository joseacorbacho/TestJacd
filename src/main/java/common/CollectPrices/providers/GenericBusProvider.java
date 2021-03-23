

package common.CollectPrices.providers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public abstract class GenericBusProvider implements BusProvider {

	private static Logger logger = LogManager.getLogger(GenericBusProvider.class);

	private static String CHAIN_SEPARATOR = "\\.";

	// TODO persistence listeners
	protected List<BusObserver> observers = Collections.synchronizedList(new ArrayList<BusObserver>());

	protected String currency, source, instrument, record;
	protected Set<String> fields;

	private HashMap<String, Object> lastFields = new HashMap<>();

	public GenericBusProvider(String currency, String source, String instrument, String record, Set<String> fields) {
		this.currency = currency;
		this.source = source;
		this.instrument = instrument;
		this.record = record;
		this.fields = fields;
	}

	public GenericBusProvider(String chain, Set<String> fields) {
		String[] splittedChain = chain.split(CHAIN_SEPARATOR);
		assert splittedChain.length == 4;
		this.currency = splittedChain[0];
		this.instrument = splittedChain[1];
		this.source = splittedChain[2];
		this.record = splittedChain[3];
		this.fields = fields;
	}

	protected abstract void initChain();
	
	protected abstract String initRecord();

	protected abstract void finish();

	@Override public boolean addObserver(BusObserver observer) {

		boolean newSubscription = false;
		if (!observers.contains(observer)) {
			newSubscription = true;
		} else {
			logger.error("Trying to addObserver {} already added ", observer);
			newSubscription = false;
		}

		if (newSubscription) {
			logger.info("addObserver {}", observer);
			observers.add(observer);
		}
		return true;

	}

	@Override public boolean removeObserver(BusObserver observer) {
		if (!observers.contains(observer)) {
			logger.error("Trying to removeObserver {} not in list", observer);
		} else {
			observers.remove(observer);
		}
		return true;
	}

	@Override public String toString() {
		return String.format("%s.%s.%s.%s", this.currency, this.instrument, this.source, this.record);
	}

	protected Map<String, Object> updateLastFields(Map<String, Object> updateData) {
		for (String field : this.fields) {
			if (updateData.containsKey(field))
				lastFields.put(field, updateData.get(field));
		}
		return lastFields;
	}

	//	Listener methods of ion

	protected boolean areNullValues(Map<String, Object> data) {
		boolean areNulls = false;
		for (Object value : data.values()) {
			if (value == null) {
				areNulls = true;
				break;
			}
		}
		return areNulls;
	}

	protected void updateObservers(Map<String, Object> data) {
		updateLastFields(data);
		List<BusObserver> auxObservers = new ArrayList<>(observers);
		boolean isFullDownloaded = false;

		if (!areNullValues(lastFields) && lastFields.size() == this.fields.size()) {
			isFullDownloaded = true;
		}

		for (BusObserver observer : auxObservers) {
			observer.update(data);
			if (isFullDownloaded) {
				observer.fullUpdate(lastFields);
			}

		}
	}
	public String getRecord() {
		return record;
	}
}
