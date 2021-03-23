package common.CollectPrices.publisher;

import com.google.cloud.pubsub.v1.Subscriber;

public class PubSubReceiverTopic extends PubSubReceiver {

	public PubSubReceiverTopic(PubSubConfiguration pubSubcfg) throws Exception {
		super(pubSubcfg);
	}

	@Override
	protected Subscriber getDestination() throws Exception {
		/**
		 * TODO:
		 * deberia devolver un subscriber
		 */
		return this.subscriber;
//		if (session != null) {
//			return session.createTopic(configuration.getName());
//		} else {
//			throw new Exception("Session does not exist");
//		}
	}
}
