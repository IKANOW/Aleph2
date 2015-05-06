package com.ikanow.aleph2.access_manager.data_access.sample_services;

import java.util.Map;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.shared.Identity;

public class SampleISecurityService implements ISecurityService {

	@Override
	public boolean hasPermission(Identity identity, Class<?> resourceClass,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasPermission(Identity identity, String resourceName,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Identity getIdentity(Map<String, Object> token) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void grantPermission(Identity identity, Class<?> resourceClass,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void grantPermission(Identity identity, String resourceName,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void revokePermission(Identity identity, Class<?> resourceClass,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void revokePermission(Identity identity, String resourceName,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearPermission(Class<?> resourceClass) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearPermission(String resourceName) {
		// TODO Auto-generated method stub

	}

}
