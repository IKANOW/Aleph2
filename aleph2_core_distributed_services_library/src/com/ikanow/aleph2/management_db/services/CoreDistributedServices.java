package com.ikanow.aleph2.management_db.services;

import org.apache.curator.framework.CuratorFramework;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICoreDistributedServices;

/** Implementation class for full Curator service
 * @author acp
 *
 */
public class CoreDistributedServices implements ICoreDistributedServices {

	 
	/** Returns a connection to the Curator server
	 * @return
	 */
	@NonNull
	public CuratorFramework getCuratorFramework() {
		return null;
	}
}
