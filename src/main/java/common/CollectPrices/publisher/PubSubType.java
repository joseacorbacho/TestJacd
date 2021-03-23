package common.CollectPrices.publisher;

public enum PubSubType {
	Topic,
	Queue;

	public static PubSubType get(String str) {
		if (str.equalsIgnoreCase("topic")) {
			return PubSubType.Topic;
		} else {
			return PubSubType.Queue;
		}
	}
}
