package common.CollectPrices.manager;

public class ElemtPriceQty {
	
	private int qty;
	
	private double price;
	
	ElemtPriceQty (double price, int qty) {
		this.setPrice(price);
		this.setQty(qty);
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public int getQty() {
		return qty;
	}

	public void setQty(int qty) {
		this.qty = qty;
	}

}
