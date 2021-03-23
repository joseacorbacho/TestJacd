package common.CollectPrices.publisher;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class SubDestination {

	protected final PubSubConfiguration configuration;
	protected final Logger logger;

	protected Subscriber subscriber = null;

	SubDestination(PubSubConfiguration config) {
		logger = LogManager.getLogger(this);
		configuration = config;
	}

	/**
	 * @throws InvalidParameterException
	 *             when the configuration has any null value
	 */
	private void validationBeforeOpen() throws InvalidParameterException {

		if (configuration.getPubSubProvider() == PubSubProvider.Google) {
			validateGoogleParameters();
		} else {
			//validateWebSphereMQParameters();
		}

	}

	private void validateGoogleParameters() throws InvalidParameterException {
		if (!isSet(configuration.getProject())) {
			throw new InvalidParameterException("project");
		}
		if (!isSet(configuration.getTopic())) {
			throw new InvalidParameterException("Topic");
		}
	}
	
	/**
	 * @return true if it isn't null or it isn't empty
	 */
	public static boolean isSet(String str) {
		return (str != null) && (!str.isEmpty());
	}

	private void validateWebSphereMQParameters() throws InvalidParameterException {
		
	}

	/**
	 * @return true if session and connection were established
	 */
	public boolean isOpen() {
		return subscriber != null;
	}

	protected void open() throws InvalidParameterException, Exception {
		logger.info("Opening");

		validationBeforeOpen();

		if (configuration.getPubSubProvider() == PubSubProvider.Google) {
			openPubSub();
		} 
//			else {
//			openWebSphereMQ();
//		}

		logger.info("Opened");
	}

	private static final BlockingQueue<PubsubMessage> messages = new LinkedBlockingDeque<>();
	 
	static class MessageReceiverExample implements MessageReceiver {

	    @Override
	    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
	      messages.offer(message);
	      consumer.ack();
	    }
	  }
	private void openPubSub() throws IOException {
		
		//int messageCount = Integer.parseInt(args[1]);
		ProjectTopicName topicName = ProjectTopicName.of(configuration.getProject(), configuration.getTopic());
	    
	    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(configuration.getProject(),
				configuration.getTopic());
	   
	    // Create a publisher instance with default settings bound to the topic
     
	    subscriber =Subscriber.newBuilder(subscriptionName, new MessageReceiverExample()).build();
	      subscriber.startAsync().awaitRunning();
	


	}

	
	protected void close() {
		if (isOpen()) {
			
			try {
				subscriber.stopAsync();
			} catch (Exception e) {
				logger.error("Error closing PubSub. " + e.getMessage());
			}				
		}
		
		subscriber = null;
		logger.info("Closed");
	}

	public PubSubConfiguration getConfiguration() {
		return configuration;
	}

	abstract protected Subscriber getDestination() throws Exception;

	@Override
	public String toString() {
		StringBuilder strb = new StringBuilder(getClass().getSimpleName());
		strb.append('(');
		strb.append(configuration.getTopic());
		strb.append(')');
		return strb.toString();
	}
}
