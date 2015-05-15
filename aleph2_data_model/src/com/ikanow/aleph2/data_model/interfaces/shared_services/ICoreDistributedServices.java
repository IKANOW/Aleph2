package com.ikanow.aleph2.data_model.interfaces.shared_services;

import org.apache.curator.framework.CuratorFramework;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Provides general access to distributed services in the cluster - eg distributed mutexes, control messaging, data queue access
 * @author acp
 */
public interface ICoreDistributedServices {

	/** Returns a connection to the Curator server
	 * @return
	 */
	@NonNull
	CuratorFramework getCuratorFramework();
}
