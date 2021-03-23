package common.CollectPrices.jms;

public class JmsConfiguration {

	private final JmsProvider jmsProvider;
	private final TypeJms typeJms;
	private final String name;
	private final String userName;
	private final String userPass;
	private final String serverUrl;
	private final String serverUrlSlave;

	// private final String hostName; --- > serverUrl, first part
	private final Integer port; // ---- > from serverUrl, second part
	private final String channel;
	private final String queueManager;
	private final String destinationName;

	public JmsConfiguration(String jmsProvider, String name, String userName, String userPass, String serverUrl,
			String serverUrlSlave, TypeJms typeJms) {
		this.jmsProvider = this.getJmsProvider(jmsProvider);

		this.name = name;
		this.userName = userName;
		this.userPass = userPass;
		this.serverUrl = serverUrl;
		this.serverUrlSlave = serverUrlSlave;
		this.typeJms = typeJms;

		this.channel = "";
		this.queueManager = "";
		this.destinationName = "";
		this.port = 0;
	}

	public JmsConfiguration(String jmsProvider, String serverUrl, TypeJms typeJms, String channel, String queueManager,
			String destinationName, Integer port) {

		this.jmsProvider = this.getJmsProvider(jmsProvider);
		this.serverUrl = serverUrl;
		this.typeJms = typeJms;
		this.channel = channel;
		this.queueManager = queueManager;
		this.destinationName = destinationName;
		this.port = port;

		this.name = "";
		this.serverUrlSlave = "";
		this.userName = "";
		this.userPass = "";
	}

	public JmsProvider getJmsProvider() {
		return jmsProvider;
	}

	public TypeJms getTypeJms() {
		return typeJms;
	}

	public String getName() {
		return name;
	}

	public String getUserName() {
		return userName;
	}

	public String getUserPass() {
		return userPass;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public String getServerUrlSlave() {
		return serverUrlSlave;
	}

	public String getChannel() {
		return channel;
	}

	public String getQueueManager() {
		return queueManager;
	}

	public String getDestinationName() {
		return destinationName;
	}

	public Integer getPort() {
		return port;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(getClass().getSimpleName());
		builder.append(" [typeJms=");
		builder.append(typeJms);
		builder.append(", name=");
		builder.append(name);
		builder.append(", userName=");
		builder.append(userName);
		builder.append(", serverUrl=");
		builder.append(serverUrl);
		builder.append(", serverUrlSlave=");
		builder.append(serverUrlSlave);
		builder.append(", port=");
		builder.append(getPort());
		builder.append(", channel=");
		builder.append(channel);
		builder.append(", queueManager=");
		builder.append(queueManager);
		builder.append(", destinationName=");
		builder.append(destinationName);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((serverUrl == null) ? 0 : serverUrl.hashCode());
		result = prime * result + ((serverUrlSlave == null) ? 0 : serverUrlSlave.hashCode());
		result = prime * result + ((typeJms == null) ? 0 : typeJms.hashCode());
		result = prime * result + ((userName == null) ? 0 : userName.hashCode());
		result = prime * result + ((userPass == null) ? 0 : userPass.hashCode());
		result = prime * result + ((getPort() == null) ? 0 : getPort().hashCode());
		result = prime * result + ((channel == null) ? 0 : channel.hashCode());
		result = prime * result + ((queueManager == null) ? 0 : queueManager.hashCode());
		result = prime * result + ((destinationName == null) ? 0 : destinationName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof JmsConfiguration)) {
			return false;
		}
		JmsConfiguration other = (JmsConfiguration) obj;
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		if (serverUrl == null) {
			if (other.serverUrl != null) {
				return false;
			}
		} else if (!serverUrl.equals(other.serverUrl)) {
			return false;
		}
		if (serverUrlSlave == null) {
			if (other.serverUrlSlave != null) {
				return false;
			}
		} else if (!serverUrlSlave.equals(other.serverUrlSlave)) {
			return false;
		}
		if (typeJms != other.typeJms) {
			return false;
		}
		if (userName == null) {
			if (other.userName != null) {
				return false;
			}
		} else if (!userName.equals(other.userName)) {
			return false;
		}
		if (userPass == null) {
			if (other.userPass != null) {
				return false;
			}
		} else if (!userPass.equals(other.userPass)) {
			return false;
		}
		if (channel == null) {
			if (other.channel != null) {
				return false;
			}
		} else if (!channel.equals(other.channel)) {
			return false;
		}
		if (queueManager == null) {
			if (other.queueManager != null) {
				return false;
			}
		} else if (!queueManager.equals(other.queueManager)) {
			return false;
		}

		if (destinationName == null) {
			if (other.destinationName != null) {
				return false;
			}
		} else if (!destinationName.equals(other.destinationName)) {
			return false;
		}
		if (getPort() == null) {
			if (other.getPort() != null) {
				return false;
			}
		} else if (!getPort().equals(other.getPort())) {
			return false;
		}
		return true;
	}

	private JmsProvider getJmsProvider(final String provider) {
		return provider.equalsIgnoreCase(JmsProvider.tibco.name()) ? JmsProvider.tibco : JmsProvider.websphereMQ;
	}
}
