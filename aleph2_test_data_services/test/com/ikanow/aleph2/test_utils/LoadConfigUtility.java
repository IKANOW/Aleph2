package com.ikanow.aleph2.test_utils;

public class LoadConfigUtility {
	
	/**
	 * This test suite is for letting other applications test adding their services to our
	 * platform and making sure all regression tests pass before they throw it on a live cluster.
	 * 
	 * Because we are using typesafe config files, you can override the config for these test cases
	 * in a couple ways to add in your own widget: https://github.com/typesafehub/config#standard-behavior
	 * 
	 * 1. You can submit system properties overriding your specific service
	 * 2. You can manually change the application.conf in this class
	 * 
	 */
	
	//TODO do we need to pass our config file from here to the other components so they use the overridden fields?
}
