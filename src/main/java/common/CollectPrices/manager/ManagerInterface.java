
package common.CollectPrices.manager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import common.CollectPrices.publisher.QueueConfiguration;

public interface ManagerInterface {
	
	public Integer sendToGoogle(String idBus, long timestamp, String type, String source, QueueConfiguration queueConfiguration, AtomicInteger messagesSent, Map<String, Object> completeFields);

}