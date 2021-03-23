package common.CollectPrices.tick;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.bind.DatatypeConverter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.annotations.Expose;

public abstract class GenericTick implements Serializable {

	private static AtomicLong ERROR_COUNTER = new AtomicLong();

	private static Logger logger = LogManager.getLogger(GenericTick.class);

	@Expose private String source;

	@Expose private String type;

	@Expose private long timestamp;

	@Expose private String instrumentId;

	@Expose private Map<String, Object> values;

	@Expose private String id;
	
	public GenericTick(String source, String type, long timestamp) {
		
		this.source = source;
		this.type = type;
		this.timestamp = timestamp;

		values = new HashMap<>();

		this.id = createUniqueId();
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setInstrumentId(String instrumentId) {
		this.instrumentId = instrumentId;
	}

	public String getId() {
		return id;
	}

	private String createUniqueId() {

		try {
			ByteArrayOutputStream byteObject = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(byteObject);
			os.writeObject(this);

			ByteArrayOutputStream byteObject1 = new ByteArrayOutputStream();
			ObjectOutputStream os1 = new ObjectOutputStream(byteObject1);
			os1.writeObject(System.nanoTime());
			byte[] object = byteObject.toByteArray();
			byte[] timestamp = byteObject1.toByteArray();
			byte[] uniqueId = new byte[object.length + timestamp.length];
			System.arraycopy(object, 0, uniqueId, 0, object.length);
			System.arraycopy(timestamp, 0, uniqueId, object.length, timestamp.length);

			//			String identifierString = Base64.getEncoder().encodeToString(uniqueId);

			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(uniqueId);
			byte[] digest = md.digest();
			String myChecksum = DatatypeConverter.printHexBinary(digest).toUpperCase();

			return myChecksum;

		} catch (IOException ex) {
			logger.error("Error createUniqueId , returning atomic long: " + ex);
			String errorId = new Date().toString() + "_" + String.valueOf(ERROR_COUNTER.getAndIncrement());
			return errorId;
		} catch (NoSuchAlgorithmException e) {
			logger.error("Error MD5 message Digest not found " + e);
			String errorId = new Date().toString() + "_" + String.valueOf(ERROR_COUNTER.getAndIncrement());
			return errorId;
		}
	}

	public void addValue(String key, Object value) {
		values.put(key, value);
	}

	public void removeValue(String key) {
		values.remove(key);
	}

	public void addValues(Map<String, Object> valueMap) {
		values.putAll(valueMap);
	}

	public String getSource() {
		return source;
	}

	public String getType() {
		return type;
	}

	public String getInstrumentId() {
		return instrumentId;
	}

	public Object getValue(String key) {
		if (values.containsKey(key)) {
			return values.get(key);
		}
		return null;
	}

	public abstract void setInvalidStatus();

	protected abstract String getDesk();

	protected abstract void setDesk(String string);

	public abstract boolean getStatus();
}