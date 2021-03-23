package common.CollectPrices.publisher;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PubSubMessagePublisher implements QueuePublisher {

	private Logger logger = LogManager.getLogger(PubSubMessagePublisher.class);

	private ExecutorService poolExecutor;
	private final Map<PubSubConfiguration, PubSubThreadSender> senders;

	@Value("${mkv.queue.publisher.threads:5}") private int threads;


	public PubSubMessagePublisher() {
		senders = new HashMap<PubSubConfiguration, PubSubMessagePublisher.PubSubThreadSender>();
	}

	@PostConstruct private void init() {

		poolExecutor = Executors.newFixedThreadPool(threads);
		logger.info("Starting {} with {} mkv.queue.publisher.threads", this.getClass().getName(), threads);

	}

	@Override public boolean send(String message, QueueConfiguration configuration, long timestamp) {
		configuration = configuration instanceof PubSubConfiguration ? ((PubSubConfiguration) configuration) : null;
		if (configuration == null) {
			logger.error("configuration is not ZeroMqConfiguration");
			return false;
		}
		PubSubConfiguration configuration1 = (PubSubConfiguration) configuration;
		PubSubThreadSender sender = getGuaranteed(configuration1);
		sender.setMessage(message);
		poolExecutor.execute(sender);

		return true;
	}

	@Override public int getErrorCounter(QueueConfiguration configuration) {
		PubSubConfiguration configuration1 = (PubSubConfiguration) configuration;
		PubSubThreadSender sender = senders.get(configuration1);
		if (sender == null) {
			return 0;
		}
		return sender.getSender().getErrorCounter().get();
	}

	@Override public int getMessagesSent(QueueConfiguration configuration) {
		PubSubConfiguration configuration1 = (PubSubConfiguration) configuration;
		PubSubThreadSender sender = senders.get(configuration1);
		if (sender == null) {
			return 0;
		}
		return sender.getSender().getSuccessCounter().get();
	}

	private PubSubThreadSender getGuaranteed(PubSubConfiguration configuration) {
		PubSubThreadSender sender = senders.get(configuration);
		if (sender == null) {
			sender = new PubSubThreadSender(PubSubFactory.createSender(configuration));
			senders.put(configuration, sender);
		}
		return sender;
	}

	class PubSubThreadSender implements Runnable {

		private final PubSubSender sender;
		// private String message;
		private Set<String> inputMessagesQueue;

		PubSubThreadSender(PubSubSender sender) {
			this.sender = sender;
			inputMessagesQueue = new HashSet<String>();
		}

		public PubSubSender getSender() {
			return sender;
		}

		public void setMessage(String message) {
			synchronized (inputMessagesQueue) {
				inputMessagesQueue.add(message);
			}
		}

		@Override public void run() {

			sendMessagesToQueue();

		}

		private void sendMessagesToQueue() {
			Set<String> auxMessagesQueue = new HashSet<String>();

			synchronized (inputMessagesQueue) {
				inputMessagesQueue.forEach(message -> auxMessagesQueue.add(message));
				inputMessagesQueue.clear();
			}

			try {
				auxMessagesQueue.forEach(message -> {
					try {
						sender.send(message);
					} catch (Exception e) {
						logger.error("PubSub error. " + e.getMessage());
					}

				});
			} catch (InvalidParameterException e) {
				logger.error("Invalid PubSub configuration.");
			}

			synchronized (inputMessagesQueue) {
				if (inputMessagesQueue.isEmpty() == false) {
					logger.debug("{} Messages waiting in queue. \n failed:{}\nsucceed:{}\nsuccessRate:{}",
							inputMessagesQueue.size(), sender.getErrorCounter().get(), sender.getSuccessCounter().get(),
							sender.getSuccessRate());
					sendMessagesToQueue();
				}
			}
		}
	}

}
