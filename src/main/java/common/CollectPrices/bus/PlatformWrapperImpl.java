package common.CollectPrices.bus;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvTransactionListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

public class PlatformWrapperImpl implements PlatformWrapper {

	private final Logger logger;
	private final Set<MkvPublishListener> mkvPublishListeners;
	private final Set<MkvTransactionListener> mkvTransactionListeners;
	private MkvPublishManager mkvPublishManager;
	private Boolean notified;

	public PlatformWrapperImpl() {
		logger = LogManager.getLogger(PlatformWrapper.class);
		mkvPublishListeners = new HashSet<MkvPublishListener>();
		mkvTransactionListeners = new HashSet<MkvTransactionListener>();
		notified = false;
	}

	@Resource
	public void setMkvPublishListeners(Set<MkvPublishListener> mkvPublishListeners) {
		this.mkvPublishListeners.addAll(mkvPublishListeners);
	}

	@Resource
	public void setMkvTransactionListeners(Set<MkvTransactionListener> mkvTransactionListeners) {
		this.mkvTransactionListeners.addAll(mkvTransactionListeners);
	}

	@PostConstruct
	public void init() {
		try {
			MkvQoS qos = new MkvQoS();

			qos.setComponentVersion(0,0,0, 0);
			qos.setPlatformListeners(new MkvPlatformListener[] { this });

			qos.setPublishListeners(mkvPublishListeners.toArray(new MkvPublishListener[mkvPublishListeners.size()]));
			qos.setTransactionListeners(
					mkvTransactionListeners.toArray(new MkvTransactionListener[mkvTransactionListeners.size()]));

			logger.info("Starting platform");

			mkvPublishManager = Mkv.start(qos).getPublishManager();

		} catch (MkvException me) {
			logger.fatal(me);
			System.exit(-1);
		}

		synchronized (this) {
			try {
				if (!notified) {
					wait();
				}
			} catch (InterruptedException ie) {
				logger.fatal(ie);
				System.exit(-1);
			}
		}
		logger.info("Platform ready");
	}

	/*
	 * MkvPlatformListener
	 */

	@Override
	public void onComponent(MkvComponent component, boolean registered) {

	}

	public void onConnect(String component, boolean connected) {

	}

	@Override
	public void onMain(MkvPlatformEvent event) {
		logger.info("Platform event received, event: " + event);

		if (MkvPlatformEvent.REGISTER_IDLE.equals(event)) {
			synchronized (this) {
				notify();
				notified = true;
			}
		} else if (MkvPlatformEvent.SHUTDOWN_REQUEST.equals(event)) {
			// TODO - Perhaps we want to react to this event to clean up some things before we receive the subsequent
			// STOP event
		} else if (MkvPlatformEvent.STOP.equals(event)) {
			logger.info("Goodbye");
			System.exit(0);
		}
	}

	/*
	 * PlatformWrapper
	 */

	@Override
	public void addMkvPublishListener(MkvPublishListener listener) {
		mkvPublishManager.addPublishListener(listener);
	}

	@Override
	public void removeMkvPublishListener(MkvPublishListener listener) {
		mkvPublishManager.removePublishListener(listener);
	}

	@Override
	public void addMkvTransactionListener(MkvTransactionListener listener) {
		mkvPublishManager.addTransactionListener(listener);
	}

	@Override
	public void removeMkvTransactionListener(MkvTransactionListener listener) {
		mkvPublishManager.removeTransactionListener(listener);
	}

	@Override
	public List getPlatformRecords() {
		return mkvPublishManager.getMkvRecords();
	}

}