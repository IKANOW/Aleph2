package com.ikanow.aleph2.data_model.interfaces.data_access.samples;

import java.util.Map;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.shared.Identity;

public class SampleSecurityService implements ISecurityService {
	
	@Override
	public boolean hasPermission(@NonNull Identity identity,
			@NonNull Class<?> resourceClass, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasPermission(@NonNull Identity identity,
			@NonNull String resourceName, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Identity getIdentity(@NonNull Map<String, Object> token)
			throws Exception {
		return null;
	}

	@Override
	public void grantPermission(@NonNull Identity identity,
			@NonNull Class<?> resourceClass, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void grantPermission(@NonNull Identity identity,
			@NonNull String resourceName, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void revokePermission(@NonNull Identity identity,
			@NonNull Class<?> resourceClass, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void revokePermission(@NonNull Identity identity,
			@NonNull String resourceName, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearPermission(@NonNull Class<?> resourceClass,
			String resourceIdentifier) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearPermission(@NonNull String resourceName,
			String resourceIdentifier) {
		// TODO Auto-generated method stub

	}
}
