package common.CollectPrices.publisher;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.*;

import java.io.FileInputStream;
import java.security.InvalidParameterException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class PubSubReceiver  extends SubDestination{

	private BlockingQueue<PubsubMessage> messages;
	private ProjectTopicName topicName = null;
	protected Subscriber subscriber = null;

	public PubSubReceiver(PubSubConfiguration configuration) throws Exception {
		super(configuration);
		this.topicName = ProjectTopicName.of(configuration.getProject(), configuration.getTopic());

		messages = new LinkedBlockingDeque<>();

	}

	private void createSubscriptionIfNotExists(ProjectTopicName topicName, String subscriptionId) throws Exception {

		// Create a new subscription
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(topicName.getProject(), subscriptionId);

		boolean condition = true;
		int counter = 0;
		while (condition) {

			try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
				// create a pull subscription with default acknowledgement deadline (= 10 seconds)
				Subscription subscription = subscriptionAdminClient
						.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
				logger.info("Created new subscription " + subscriptionName.getSubscription());
			} catch (ApiException e) {
				// example : code = ALREADY_EXISTS(409) implies subscription already exists
				if (e.getStatusCode().getCode().getHttpStatusCode() != 409) {
					logger.error("Error creating subscription " + e.getStatusCode().getCode());
					logger.error("Error creating is isRetryable " + e.isRetryable());
					if (counter > 5) {
						condition = false;
					} else {
						Thread.sleep(500);
						counter++;
					}

				} else {
					logger.debug("Subscription " + subscriptionName.getSubscription() + " already exists");
					condition = false;
				}
			}

		}

	}



	@Override
	protected void open() throws Exception, InvalidParameterException {
		if (isOpen()) {
			logger.warn("Already is open");
		} else {

			try {
				this.subscriber = createSubscriber();
				//				createSubscriptionIfNotExists(this.topicName, this.configuration.getSubscriptionId());
			} catch (Exception e) {
				logger.error("Error creating subscription to project:{0} topic:{1} subscriptionId{2}",
						new Object[] { this.configuration.getProject(), this.configuration.getTopic(),
								this.configuration.getSubscriptionId() });
				throw e;
			}

			//consumer = session.createConsumer(destination);
		}
	}

	private Subscriber createSubscriber() throws Exception {
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName
				.of(topicName.getProject(), this.configuration.getSubscriptionId());
		// create a subscriber bound to the asynchronous message receiver

		CredentialsProvider credentialsProvider = FixedCredentialsProvider
				.create(ServiceAccountCredentials.fromStream(new FileInputStream(configuration.getCredentialsJson())));

		logger.debug("Creating subscriber to project{}, subscribeId {} with credentials {}", configuration.getProject(),
				configuration.getSubscriptionId(), configuration.getCredentialsJson());

		subscriber = Subscriber.newBuilder(subscriptionName, new MessageReceiverExample())
				.setCredentialsProvider(credentialsProvider).build();

		subscriber.startAsync().awaitRunning();

		return subscriber;

	}

	@Override
	public void close() {
		if (isOpen()) {
			try {
				subscriber.stopAsync();
				super.close();
			} catch (Exception e) {
				logger.info("Error closing consumer.");
			}
			subscriber = null;

		} else {
			logger.warn("Already is closed");
		}
	}

	/**
	 * @see JmsDestination
	 * @return true if JmsDestination is open and the consumer was created
	 * */
	@Override
	public boolean isOpen() {
		return subscriber != null;
	}

	public PubsubMessage  receiveMsg() throws Exception {
		if (!isOpen()) {
			open();
		}

		logger.debug("Waiting to receive a message...");
		PubsubMessage message = messages.take();
		logger.debug("Message received: " + message.getMessageId());

		return message;

	}

	private class MessageReceiverExample implements MessageReceiver {

		@Override public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
			messages.offer(message);
			consumer.ack();
		}
	}
}
