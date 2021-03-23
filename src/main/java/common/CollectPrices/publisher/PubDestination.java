package common.CollectPrices.publisher;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectTopicName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.bp.Duration;

import java.io.FileInputStream;
import java.security.InvalidParameterException;

abstract public class PubDestination {

	protected final PubSubConfiguration configuration;
	protected final Logger logger;

	protected Publisher publisher = null;
	//	List<ApiFuture<String>> futures;

	PubDestination(PubSubConfiguration config) {
		logger = LogManager.getLogger(this);
		configuration = config;
		//		futures = new ArrayList<>();
	}



	/**
	 * @throws InvalidParameterException when the configuration has any null value
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
		return publisher != null;
	}

	protected void open() throws InvalidParameterException, Exception {
		logger.info("Opening");

		validationBeforeOpen();

		if (configuration.getPubSubProvider() == PubSubProvider.Google) {
			openPubSub();
		}

		logger.info("Opened");
	}

	private void createTopicIfnotExists(ProjectTopicName topicName) throws Exception {
		boolean condition = true;
		int counter = 0;
		while (condition) {
				/*
				  - Entorno: sentinel-santander-dev1
  				  - Tópico: projects/sentinel-santander-dev1/topics/dev1-test-pub-sub
				 */
			try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
				logger.info("Creating topic " + topicName.getTopic() + " in project " + topicName.getProject());
				topicAdminClient.createTopic(topicName);
				condition = false;
			} catch (ApiException e) {
				if (e.getStatusCode().getCode().getHttpStatusCode() != 409) {
					// example : code = ALREADY_EXISTS(409) implies topic already exists
					logger.error("Error No connection ");
					logger.error("Error creating topic:" + e.getStatusCode().getCode());
					logger.error("Error creating topic is isRetryable:" + e.isRetryable());
					if (counter > 5) {
						condition = false;
					} else {
						Thread.sleep(500);
						counter++;
					}

				} else {
					condition = false;
				}
			}
		}

	}

	private ExecutorProvider getExecutorProviderPerTopic() {
		ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(2)
				.build();
		return executorProvider;
	}

	private RetrySettings getRetrySettingsPerTopic() {
		// Retry settings control how the publisher handles retryable failures
		Duration retryDelay = Duration.ofMillis(500); // default: 5 ms
		double retryDelayMultiplier = 2.0; // back off for repeated failures, default: 2.0
		Duration maxRetryDelay = Duration.ofSeconds(600); // default : Long.MAX_VALUE
		Duration totalTimeout = Duration.ofSeconds(120); // default: 10 seconds
		Duration initialRpcTimeout = Duration.ofSeconds(120); // default: 10 seconds
		Duration maxRpcTimeout = Duration.ofSeconds(120); // default: 10 seconds

		RetrySettings retrySettings = RetrySettings.newBuilder().setInitialRetryDelay(retryDelay)
				.setRetryDelayMultiplier(retryDelayMultiplier).setMaxRetryDelay(maxRetryDelay)
				.setTotalTimeout(totalTimeout).setInitialRpcTimeout(initialRpcTimeout).setMaxRpcTimeout(maxRpcTimeout)
				.build();

		return retrySettings;

	}

	private void openPubSub() throws Exception {

		//int messageCount = Integer.parseInt(args[1]);
		ProjectTopicName topicName = ProjectTopicName.of(configuration.getProject(), configuration.getTopic());

		//create the topic if not exists
		//		createTopicIfnotExists(topicName);

		// Create a publisher instance with default settings bound to the topic
		CredentialsProvider credentialsProvider = FixedCredentialsProvider
				.create(ServiceAccountCredentials.fromStream(new FileInputStream(configuration.getCredentialsJson())));

		logger.debug("Opening pubsub to project{}, topic {} with credentials {}", configuration.getProject(),
				configuration.getTopic(), configuration.getCredentialsJson());

		//https://cloud.google.com/pubsub/docs/publisher

		publisher = Publisher.newBuilder(topicName).setCredentialsProvider(credentialsProvider)
				//				.setRetrySettings(getRetrySettingsPerTopic())
				.setExecutorProvider(getExecutorProviderPerTopic())//2 threads per topic
				.build();
	}

	protected void close() {
		if (isOpen()) {

			try {

				// Wait on any pending requests
				//				logger.debug("Waiting to send pending " + futures.size() + " messages ");
				//				List<String> messageIds = ApiFutures.allAsList(futures).get();

				//				for (String messageId : messageIds) {
				//					logger.debug("Message pending send:" + messageId);
				//				}

				publisher.shutdown();
			} catch (Exception e) {
				logger.error("Error closing PubSub. " + e.getMessage());
			}
		}

		publisher = null;
		logger.info("Closed");
	}

	public PubSubConfiguration getConfiguration() {
		return configuration;
	}

	abstract protected Publisher getDestination() throws Exception;

	@Override public String toString() {
		StringBuilder strb = new StringBuilder(getClass().getSimpleName());
		strb.append('(');
		strb.append(configuration.getTopic());
		strb.append(')');
		return strb.toString();
	}

}
