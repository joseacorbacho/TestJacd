package common.CollectPrices.tick;

import com.google.gson.annotations.Expose;
 
public class SourceTick extends GenericTick {

	@Expose private String desk;

	@Expose private Boolean status;

	public SourceTick(String desk, String source, String type, long timestamp) {
		super(source, type, timestamp);
		this.desk = desk;
		this.status = true;
	}
	
	public void setDesk(String desk) {
		this.desk = desk;
	}

	public void setInvalidStatus() {
		status = false;
	}

	public String getDesk() {
		return desk;
	}

	public boolean getStatus() {
		return status;
	}

}