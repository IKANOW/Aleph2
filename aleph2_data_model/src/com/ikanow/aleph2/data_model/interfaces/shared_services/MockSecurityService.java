package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/** A useful mock object for unit testing. Allows everything
 *  TODO (ALEPH-31): provide an interface to add overrides for testing
 * @author acp
 *
 */
public class MockSecurityService implements ISecurityService {

	/** Mock security subject
	 * @author Alex
	 */
	public static class MockSubject implements ISubject {

		@Override
		public Object getSubject() {
			return this;
		}

		@Override
		public boolean isAuthenticated() {
			return true;
		}
		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Collections.emptyList();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#login(java.lang.String, java.lang.Object)
	 */
	@Override
	public ISubject login(String principalName, Object credentials) {
		return new MockSubject();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#hasRole(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject, java.lang.String)
	 */
	@Override
	public boolean hasRole(ISubject subject, String role) {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#isPermitted(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject, java.lang.String)
	 */
	@Override
	public boolean isPermitted(ISubject subject, String string) {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#runAs(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject, java.util.Collection)
	 */
	@Override
	public void runAs(ISubject subject, Collection<String> principals) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#releaseRunAs(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject)
	 */
	@Override
	public Collection<String> releaseRunAs(ISubject subject) {
		return Collections.emptyList();
	}

	@Override
	public ISubject getSubject() {
		return new MockSubject();
	}

}
