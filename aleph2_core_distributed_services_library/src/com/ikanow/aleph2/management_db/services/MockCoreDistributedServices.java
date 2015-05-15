package com.ikanow.aleph2.management_db.services;

import org.apache.curator.framework.CuratorFramework;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICoreDistributedServices;

/** Implementation class for standalone Curator instance
 * @author acp
 *
 */
public class MockCoreDistributedServices implements ICoreDistributedServices {

	/** Guice-invoked constructor
	 */
	@Inject
	public MockCoreDistributedServices() {
		
	}
	
	 
	/** Returns a connection to the Curator server
	 * @return
	 */
	@NonNull
	public CuratorFramework getCuratorFramework() {
		return null;
	}
}
