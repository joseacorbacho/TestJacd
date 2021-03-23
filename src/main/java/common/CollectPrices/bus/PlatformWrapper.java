package common.CollectPrices.bus;

import java.util.List;

import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvTransactionListener;

public interface PlatformWrapper extends MkvPlatformListener {

	public abstract void addMkvPublishListener(MkvPublishListener listener);

	public abstract void removeMkvPublishListener(MkvPublishListener listener);

	public abstract void addMkvTransactionListener(MkvTransactionListener listener);

	public abstract void removeMkvTransactionListener(MkvTransactionListener listener);

	public List getPlatformRecords();
}