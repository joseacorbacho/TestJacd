package common.CollectPrices.publisher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

public class PubSubConfiguration implements QueueConfiguration {

	public static final String ENVIRONMENT_SETTING_CREDENTIALS_FILE = "GOOGLE_APPLICATION_CREDENTIALS";
	private final PubSubProvider pubSubProvider;
	private final PubSubType typePubSub;
	private final String topic;
	private final String subscriptionId;
	private final String project;
	protected final Logger logger;

	public String getSubscriptionId() {
		return subscriptionId;
	}

	private String credentialsJson;

	public PubSubConfiguration(String credentialsJson, String topic, String subscriptionId, String project,
			PubSubType typePubSub) {
		logger = LogManager.getLogger(this);
		this.subscriptionId = subscriptionId;
		this.pubSubProvider = PubSubProvider.Google;
		this.topic = topic;
		this.project = project;
		this.typePubSub = typePubSub;
		this.credentialsJson = checkJsonFile(credentialsJson);
	}

	private String checkJsonFile(String credentialsJsonPath) {
		String output = "";

		File f = new File(credentialsJsonPath);
		if (f.exists() && !f.isDirectory()) {
			// set environment setting
			logger.info("found credentials file to path that exists " + credentialsJsonPath);
			output = credentialsJsonPath;
		} else {
			logger.error("not found path to credentials file  " + credentialsJsonPath + " trying with environment");
			output = checkCredentialsPath();
		}
		return output;

	}

	private String checkCredentialsPath() {
		String output = "";
		String credentialsJsonPath = System.getenv(ENVIRONMENT_SETTING_CREDENTIALS_FILE);
		if (credentialsJsonPath != null) {
			File f = new File(credentialsJsonPath);
			if (f.exists() && !f.isDirectory()) {
				// set environment setting
				logger.info(ENVIRONMENT_SETTING_CREDENTIALS_FILE + " set to path that exists " + credentialsJsonPath);
				output = credentialsJsonPath;
			} else {
				logger.error(
						ENVIRONMENT_SETTING_CREDENTIALS_FILE + " set to path that NOT exists " + credentialsJsonPath);
			}

		} else {
			logger.error(ENVIRONMENT_SETTING_CREDENTIALS_FILE + "not set to have access to gcloud");
		}
		return output;
	}

	public PubSubProvider getPubSubProvider() {
		return pubSubProvider;
	}

	public PubSubType getPubSubTypes() {
		return typePubSub;
	}

	public String getTopic() {
		return topic;
	}

	public String getProject() {
		return project;
	}

	@Override public String toString() {
		StringBuilder builder = new StringBuilder(getClass().getSimpleName());
		builder.append(" [PubSubTypeJms=");
		builder.append(typePubSub);
		builder.append(", credentials=");
		builder.append(credentialsJson);
		builder.append(", topic=");
		builder.append(topic);
		builder.append(", project=");
		builder.append(project);
		builder.append(", subscriptionId=");
		builder.append(subscriptionId);
		builder.append("]");
		return builder.toString();
	}

	@Override public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		result = prime * result + ((typePubSub == null) ? 0 : typePubSub.hashCode());
		result = prime * result + ((project == null) ? 0 : project.hashCode());
		result = prime * result + ((subscriptionId == null) ? 0 : subscriptionId.hashCode());
		result = prime * result + ((credentialsJson == null) ? 0 : credentialsJson.hashCode());
		return result;
	}

	@Override public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof PubSubConfiguration)) {
			return false;
		}
		PubSubConfiguration other = (PubSubConfiguration) obj;
		if (topic == null) {
			if (other.topic != null) {
				return false;
			}
		} else if (!topic.equals(other.topic)) {
			return false;
		}

		if (typePubSub != other.typePubSub) {
			return false;
		}
		if (project == null) {
			if (other.project != null) {
				return false;
			}
		} else if (!project.equals(other.project)) {
			return false;
		}
		if (subscriptionId == null) {
			if (other.subscriptionId != null) {
				return false;
			}
		} else if (!subscriptionId.equals(other.subscriptionId)) {
			return false;
		}
		if (credentialsJson == null) {
			if (other.credentialsJson != null) {
				return false;
			}
		} else if (!credentialsJson.equals(other.credentialsJson)) {
			return false;
		}

		return true;
	}

	private PubSubProvider getPubSubProvider(final String provider) {
		return PubSubProvider.Google;
	}

	public String getCredentialsJson() {
		return credentialsJson;
	}
}
