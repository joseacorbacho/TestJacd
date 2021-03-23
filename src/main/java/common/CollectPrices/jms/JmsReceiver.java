package common.CollectPrices.jms;

import java.security.InvalidParameterException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

abstract public class JmsReceiver extends JmsDestination {

	public JmsReceiver(JmsConfiguration config) {
		super(config);
	}

	protected MessageConsumer consumer = null;

	@Override
	protected void open() throws JMSException, InvalidParameterException {
		if (isOpen()) {
			logger.warn("Already is open");
		} else {
			super.open();
			consumer = session.createConsumer(destination);
		}
	}

	@Override
	public void close() {
		if (isOpen()) {
			try {
				consumer.close();
				super.close();
			} catch (JMSException e) {
				logger.info("Error closing consumer.");
			}
			consumer = null;

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
		return super.isOpen() && consumer != null;
	}

	public Message receiveMsg() throws JMSException {
		if (!isOpen()) {
			open();
		}

		logger.debug("Waiting to receive a message...");

		Message msgJms = consumer.receive();

		logger.debug("Message received");

		return msgJms;

	}
}
