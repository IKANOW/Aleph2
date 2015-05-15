package com.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.shared.Identity;

public class SampleSecurityService implements ISecurityService, IExtraDependencyLoader {

	public IServiceContext serviceContext; 
	
	@Inject
	public SampleSecurityService(IServiceContext serviceContext) {
		this.serviceContext = serviceContext;
	}
	
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList(new SampleSecurityServiceModule());
	}
	
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
		System.out.println("hi");
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

	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// TODO Auto-generated method stub
		
	}

}
