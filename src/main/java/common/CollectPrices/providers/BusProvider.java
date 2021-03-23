package common.CollectPrices.providers;

public interface BusProvider {

	boolean addObserver(BusObserver observer);

	boolean removeObserver(BusObserver observer);

}
