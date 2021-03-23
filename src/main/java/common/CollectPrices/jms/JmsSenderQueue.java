package common.CollectPrices.jms;

import javax.jms.Destination;
import javax.jms.JMSException;

public class JmsSenderQueue extends JmsSender {

	public JmsSenderQueue(JmsConfiguration config) {
		super(config);
	}

	@Override
	protected Destination getDestination() throws JMSException {
		if (session != null) {
			return session.createQueue(configuration.getName());
		} else {
			throw new JMSException("Session does not exist");
		}
	}
}
