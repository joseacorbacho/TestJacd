package common.CollectPrices.jms;


public class JmsFactory {

	/**
	 * @return a JMS receiver
	 * */
	public static JmsReceiver createReceiver(JmsConfiguration jmsCfgs) {
		if (jmsCfgs.getTypeJms() == TypeJms.Queue) {
			return new JmsReceiverQueue(jmsCfgs);
		} else {
			return new JmsReceiverTopic(jmsCfgs);
		}
	}

	/**
	 * @return a JMS sender
	 * */
	public static JmsSender createSender(JmsConfiguration jmsCfgs) {
		if (jmsCfgs.getTypeJms() == TypeJms.Queue) {
			return new JmsSenderQueue(jmsCfgs);
		} else {
			return new JmsSenderTopic(jmsCfgs);
		}
	}
}
