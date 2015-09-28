package com.ikanow.aleph2.security.interfaces;

import java.util.Collection;

public interface IClearableRealmCache {

	void clearAuthorizationCached(Collection<String> principalNames);

	void clearAllCaches();

}
