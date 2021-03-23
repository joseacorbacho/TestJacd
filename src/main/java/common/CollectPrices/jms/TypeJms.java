package common.CollectPrices.jms;

public enum TypeJms {
	Topic,
	Queue;

	public static TypeJms get(String str) {
		if (str.equalsIgnoreCase("topic")) {
			return TypeJms.Topic;
		} else {
			return TypeJms.Queue;
		}
	}
}
