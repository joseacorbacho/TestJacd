package common.CollectPrices.publisher;



public class PubSubFactory {
	/**
	 * @return a JMS receiver
	 * */
	public static PubSubReceiver createReceiver(PubSubConfiguration pubSubCfgs) throws Exception {

		if (pubSubCfgs.getPubSubTypes() == PubSubType.Topic) {
			return new PubSubReceiverTopic(pubSubCfgs);
		} else {
			return null;
		}

	}

	/**
	 * @return a JMS sender
	 * */
	public static PubSubSender createSender(PubSubConfiguration pubSubCfgs) {
		if (pubSubCfgs.getPubSubTypes() == PubSubType.Topic) {
			return new PubSubSenderTopic(pubSubCfgs);
		} 
		else {
			return null;
			//return new PubSubSenderQueue(jmsCfgs);
		}
	}
}
