package common.CollectPrices.jms;

import javax.jms.Destination;
import javax.jms.JMSException;

public class JmsSenderTopic extends JmsSender {

	public JmsSenderTopic(JmsConfiguration jmsCfg) {
		super(jmsCfg);
	}

	@Override
	protected Destination getDestination() throws JMSException {
		if (session != null) {
			return session.createTopic(configuration.getName());
		} else {
			throw new JMSException("Session does not exist");
		}
	}

}
