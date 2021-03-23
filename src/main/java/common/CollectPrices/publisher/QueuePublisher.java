
package common.CollectPrices.publisher;

public interface QueuePublisher {

	boolean send(String message, QueueConfiguration configuration, long timestamp);

	int getMessagesSent(QueueConfiguration configuration);

	int getErrorCounter(QueueConfiguration configuration);

}
