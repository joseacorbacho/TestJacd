
package common.CollectPrices.publisher;

import java.io.IOException;

public interface QueueListener {

	void onReceive(String message, long timestamp, QueueConfiguration from) throws IOException;

	//	void onReceive(ByteBuffer message, long timestamp) throws IOException;

}
