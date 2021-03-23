package common.CollectPrices.providers;

import java.util.Map;

public interface BusObserver {

	void update(Map<String, Object> fields);

	void fullUpdate(Map<String, Object> fields);

}
