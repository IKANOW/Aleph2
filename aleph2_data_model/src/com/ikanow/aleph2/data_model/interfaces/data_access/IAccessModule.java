package com.ikanow.aleph2.data_model.interfaces.data_access;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Interface that applications wanted to consume data services of the
 * Aleph platform should inherit.  Once the application has been loaded,
 * initialize will be called via the AccessManager passing the current 
 * IAccessContext object that will give access to the currently loaded data services.
 * 
 * @author Burch
 *
 */
public interface IAccessModule {
	/**
	 * This method will be called once when an application is
	 * loaded into the Aleph platform.  The IAccessContext should
	 * be saved locally so it can be accessed as needed.  The access
	 * context can be retrieved alternatively via 
	 * {@link com.ikanow.aleph2.data_model.utils.ContextUtils.getAccessContext()}
	 * 
	 * @param accessContext
	 */
	public void initialize(@NonNull IAccessContext accessContext);
}
