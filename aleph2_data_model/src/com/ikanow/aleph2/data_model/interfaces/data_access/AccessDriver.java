package com.ikanow.aleph2.data_model.interfaces.data_access;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * This class reads the IAccessContext set up in the config file
 * and loads an instance of that class.
 * 
 * @author Burch
 *
 */
public class AccessDriver extends AbstractModule {
	
	private static IAccessContext accessContext = null;
	
	/**
	 * The first time this class is called it will create the access context
	 */
	static {
		Injector injector = Guice.createInjector(new AccessDriver());
		accessContext = injector.getInstance(IAccessContext.class);
	}
	
	/**
	 * Kicks off the injection by reading which access context we
	 * should bind via the config file and creating an instance of that context.
	 * 
	 * @return
	 */
	public static IAccessContext getAccessContext() {
		return accessContext;
	}

	private void loadAccessContextFromConfig() throws Exception {
		Config config = ConfigFactory.load();		
		String service = config.getString("access_manager.service");
		//Convert string class name to actual class and bind to IAccessContext
		@SuppressWarnings("unchecked")
		Class<? extends IAccessContext> class2 = (Class<? extends IAccessContext>) Class.forName((String) service);
		System.out.println("Binding IAccessContext to " + class2.getCanonicalName());
		bind(IAccessContext.class).to(class2);		
	}

	@Override
	protected void configure() {
		try {
			loadAccessContextFromConfig();
		} catch (Exception e){
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
