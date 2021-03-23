package common.CollectPrices.spring;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@Component public class PropertyPlaceholderExposer implements Map<String, String>, BeanFactoryAware {

	ConfigurableBeanFactory beanFactory;

	@Override public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = (ConfigurableBeanFactory) beanFactory;
	}

	protected String resolveProperty(String name) {
		String rv = beanFactory.resolveEmbeddedValue("${" + name + "}");

		return rv;
	}

	@Override public String get(Object key) {
		return resolveProperty(key.toString());
	}

	@Override public boolean containsKey(Object key) {
		try {
			resolveProperty(key.toString());
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	@Override public boolean isEmpty() {
		return false;
	}

	@Override public Set<String> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override public Set<java.util.Map.Entry<String, String>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override public Collection<String> values() {
		throw new UnsupportedOperationException();
	}

	@Override public int size() {
		throw new UnsupportedOperationException();
	}

	@Override public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override public String put(String key, String value) {
		throw new UnsupportedOperationException();
	}

	@Override public String remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override public void putAll(Map<? extends String, ? extends String> t) {
		throw new UnsupportedOperationException();
	}

	@Override public String toString() {
		return "PropertyPlaceholderExposer{}";
	}
}
