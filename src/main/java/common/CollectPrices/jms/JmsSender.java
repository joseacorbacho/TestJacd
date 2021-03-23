package common.CollectPrices.jms;

import java.security.InvalidParameterException;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

public abstract class JmsSender extends JmsDestination {

	public JmsSender(JmsConfiguration config) {
		super(config);

		errorCounter = 1;
	}

	protected MessageProducer producer = null;

	private Integer errorCounter;
	private final int MAX_TRIES = 5;

	@Override
	protected void open() throws JMSException, InvalidParameterException {
		if (isOpen()) {
			logger.warn("It was already opened");
		} else {
			super.open();
			producer = session.createProducer(destination);
		}
	}

	@Override
	protected void close() {
		if (isOpen()) {
			try {
				producer.close();
			} catch (JMSException e) {
				logger.info("Error colosing connection.");
			}
			producer = null;

			super.close();
		} else {
			logger.warn("It was already closed");
		}
	}

	@Override
	public boolean isOpen() {
		return super.isOpen() && producer != null;
	}

	public synchronized void send(String message) throws InvalidParameterException {
		if (!isOpen()) {
			try {
				open();
			} catch (JMSException e) {
				logger.info("Error openning connection JMS");
			}
		}

		sendMessage(message);
	}

	private void sendMessage(String message) {
		TextMessage jmsMessage;
		try {
			jmsMessage = session.createTextMessage(message);

			logger.info("Sending message: " + message.length() + "Bytes");
			producer.send(jmsMessage);
			logger.info("Message sent");
			logger.debug(message);
			errorCounter = 1;
		} catch (JMSException e) {
			if (errorCounter < MAX_TRIES) {
				logger.info("Error sending message. Reopen connection. Tries: " + errorCounter.toString());
				logger.info(configuration.toString());
				errorCounter++;
				close();
				send(message);
			} else {
				logger.error("Unable to send message: " + message);
			}

		}

	}

}
