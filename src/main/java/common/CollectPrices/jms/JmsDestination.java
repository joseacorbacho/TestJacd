package common.CollectPrices.jms;

import java.security.InvalidParameterException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import com.tibco.tibjms.TibjmsConnectionFactory;

abstract public class JmsDestination {

	// TODO we can configure if the connection is transactional, we just receive this configurable attribute in
	// constructor
	private static final boolean isTransactional = false;

	protected final JmsConfiguration configuration;
	protected final Logger logger;

	protected Connection conn = null;
	protected Destination destination = null;
	protected Session session = null;

	JmsDestination(JmsConfiguration config) {
		logger = LogManager.getLogger(this);
		configuration = config;
	}

	/**
	 * @throws InvalidParameterException
	 *             when the configuration has any null value
	 */
	private void validationBeforeOpen() throws InvalidParameterException {

		if (configuration.getJmsProvider() == JmsProvider.tibco) {
			validateTibcoParameters();
		} else {
			validateWebSphereMQParameters();
		}

	}

	private void validateTibcoParameters() throws InvalidParameterException {
		if (!isSet(configuration.getServerUrl())) {
			throw new InvalidParameterException("server");
		}
		if (!isSet(configuration.getServerUrlSlave())) {
			throw new InvalidParameterException("server slave");
		}
		if (!isSet(configuration.getUserName())) {
			throw new InvalidParameterException("user");
		}
		if (!isSet(configuration.getUserPass())) {
			throw new InvalidParameterException("pass");
		}
		if (!isSet(configuration.getName())) {
			throw new InvalidParameterException("name");
		}
	}

	private void validateWebSphereMQParameters() throws InvalidParameterException {
		if (!isSet(configuration.getServerUrl())) {
			throw new InvalidParameterException("server");
		}
		if (!isSet(configuration.getPort())) {
			throw new InvalidParameterException("Port");
		}
		if (!isSet(configuration.getChannel())) {
			throw new InvalidParameterException("Channel");
		}

		if (!isSet(configuration.getQueueManager())) {
			throw new InvalidParameterException("QueueManager");
		}
		if (!isSet(configuration.getDestinationName())) {
			throw new InvalidParameterException("destinationName");
		}
	}
	
	/**
	 * @return true if it isn't null or it isn't empty
	 */
	public static boolean isSet(String str) {
		return (str != null) && (!str.isEmpty());
	}
	
	/**
	 * @return true if it isn't null
	 */
	public static boolean isSet(Integer integer) {
		return (integer != null);
	}

	/**
	 * @return true if session and connection were established
	 */
	public boolean isOpen() {
		return session != null && conn != null;
	}

	protected void open() throws JMSException, InvalidParameterException {
		logger.info("Opening");

		validationBeforeOpen();

		if (configuration.getJmsProvider() == JmsProvider.tibco) {
			openTibco();
		} else {
			openWebSphereMQ();
		}

		logger.info("Opened");
	}

	private void openTibco() throws JMSException {
		ConnectionFactory factory = new TibjmsConnectionFactory(configuration.getServerUrl() + " , "
				+ configuration.getServerUrlSlave());

		conn = factory.createConnection(configuration.getUserName(), configuration.getUserPass());
		conn.start();

		session = conn.createSession(isTransactional, Session.AUTO_ACKNOWLEDGE);

		destination = getDestination();
	}

	private void openWebSphereMQ() throws JMSException {
		JmsFactoryFactory jmsFactoryFactory = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
		JmsConnectionFactory jmsConnectionFactory = jmsFactoryFactory.createConnectionFactory();
		// Set the properties
		jmsConnectionFactory.setStringProperty(WMQConstants.WMQ_HOST_NAME, configuration.getServerUrl());
		jmsConnectionFactory.setIntProperty(WMQConstants.WMQ_PORT, configuration.getPort());
		jmsConnectionFactory.setStringProperty(WMQConstants.WMQ_CHANNEL, configuration.getChannel());
		jmsConnectionFactory.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
		jmsConnectionFactory.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, configuration.getQueueManager());

		// Create JMS objects
		conn = jmsConnectionFactory.createConnection();
		session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

		if (configuration.getTypeJms() == TypeJms.Queue) {
			destination = session.createQueue(configuration.getDestinationName());
		} else {
			destination = session.createTopic(configuration.getDestinationName());
		}
		/* MessageProducer producer = */session.createProducer(destination);
		conn.start();
	}

	protected void close() {
		if (isOpen()) {
			try {
				if (isTransactional) {
					session.commit();
				}
				session.close();

			} catch (JMSException e) {
				logger.info("Error closing destination session.");
			}

			try {
				conn.close();
			} catch (JMSException e) {
				logger.info("Error closing destination connection.");
			}
		}
		session = null;
		conn = null;
		logger.info("Closed");
	}

	public JmsConfiguration getConfiguration() {
		return configuration;
	}

	abstract protected Destination getDestination() throws JMSException;

	@Override
	public String toString() {
		StringBuilder strb = new StringBuilder(getClass().getSimpleName());
		strb.append('(');
		strb.append(configuration.getServerUrl());
		strb.append(')');
		return strb.toString();
	}

}
