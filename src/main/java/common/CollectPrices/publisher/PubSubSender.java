package common.CollectPrices.publisher;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import java.security.InvalidParameterException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PubSubSender extends PubDestination {

	private AtomicInteger successCounter;
	private AtomicInteger totalCounter;
	private AtomicInteger errorCounter;
	private double successRate;

	PubSubSender(PubSubConfiguration config) {
		super(config);

		successCounter = new AtomicInteger();
		totalCounter = new AtomicInteger();
		errorCounter = new AtomicInteger();
		successRate = 1.;
	}

	private final int MAX_TRIES = 5;

	@Override protected void open() throws InvalidParameterException, Exception {
		if (isOpen()) {
			logger.warn("It was already opened");
		} else {
			super.open();

		}
	}

	public AtomicInteger getSuccessCounter() {
		return successCounter;
	}

	public AtomicInteger getTotalCounter() {
		return totalCounter;
	}

	public AtomicInteger getErrorCounter() {
		return errorCounter;
	}

	public double getSuccessRate() {
		return successRate;
	}

	@Override protected void close() {
		if (isOpen()) {
			super.close();
		} else {
			logger.warn("It was already closed");
		}
	}

	@Override public boolean isOpen() {
		return super.isOpen();
	}

	public synchronized void send(String message) throws Exception {
		if (!isOpen()) {
			open();
		}

		sendMessage(message);
	}

	private void sendMessage(String message) {

		// convert message to bytes
		ByteString data = ByteString.copyFromUtf8(message);
		PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

		// Schedule a message to be published. Messages are automatically batched.
		ApiFuture<String> future = publisher.publish(pubsubMessage);
		//		super.futures.add(future);

		// Add an asynchronous callback to handle success / failure
		ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

			//			boolean output=false;
			@Override public void onFailure(Throwable throwable) {
				if (throwable instanceof ApiException) {
					ApiException apiException = ((ApiException) throwable);
					// details on the API exception
					logger.error("Error publishing message : " + message);
					logger.error("Google Error code:" + apiException.getStatusCode().getCode());
					logger.error("Google Error isRetryable:" + apiException.isRetryable());
				} else {
					logger.error("Error publishing message : " + message);
				}
				int fail = errorCounter.incrementAndGet();
				int total = totalCounter.incrementAndGet();
				successRate = (double) successCounter.get() / (double) total;

			}

			@Override public void onSuccess(String messageId) {
				// Once published, returns server-assigned message ids (unique within the topic)
				logger.trace("Message succesfully sent id:" + messageId);
				int success = successCounter.incrementAndGet();
				int total = totalCounter.incrementAndGet();
				successRate = (double) success / (double) total;
			}
		}, MoreExecutors.directExecutor());

	}

	private void printStats() {
		logger.trace("successes: {}", successCounter.get());
		logger.trace("errors: {}", errorCounter.get());
		logger.trace("total: {}", totalCounter.get());
		logger.trace("successRate: {}", successRate);
	}
}


