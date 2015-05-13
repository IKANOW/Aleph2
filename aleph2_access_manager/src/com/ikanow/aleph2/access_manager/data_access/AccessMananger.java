package com.ikanow.aleph2.access_manager.data_access;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Handles setting up all the bindings managed by the config files.
 * 
 * @author Burch
 *
 */
public class AccessMananger {

	public static AccessContext initialize(@NonNull Config config) {
		//get optional modules from config file, create injector
		Injector injector = Guice.createInjector(new AccessModule(config));
		AccessContext ac = new AccessContext(injector);
		ContextUtils.setAccessContext(ac);
		return ac;
	}
	
	public static void main(String[] args) {
		AccessMananger.initialize(ConfigFactory.load());
	}
}
