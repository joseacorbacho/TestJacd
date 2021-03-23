package common.CollectPrices.publisher;


import com.google.cloud.pubsub.v1.Publisher;

public class PubSubSenderTopic extends PubSubSender{
	public PubSubSenderTopic(PubSubConfiguration jmsCfg) {
		super(jmsCfg);
	}

	@Override
	protected Publisher getDestination() throws Exception  {
		if (publisher != null) {			
			return publisher;
		} else {
			throw new Exception("PubSub publisher does not exist");
		}
	}
}
