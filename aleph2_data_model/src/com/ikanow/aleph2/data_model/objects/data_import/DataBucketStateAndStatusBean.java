package com.ikanow.aleph2.data_model.objects.data_import;

import java.util.Date;
import java.util.Map;

/** Represents generic harvest state
 * @author acp
 */
public class DataBucketStateAndStatusBean {

	//TODO keep these separate from the bucket in a different "collection"
	//TODO suspended/resume/quarantined
	//TODO last harvested by
	//TODO object last inserted data
	//TODO num objects
	
	/** If non-null, this bucket has been quarantined and will not be processed until the 
	 *  specified date (at which point quarantined_until will be set to null)
	 * @return the quarantined_until
	 */
	public Date quarantined_until() {
		return quarantined_until;
	}
	private Date quarantined_until;
	private Map<String, String> log_messages;
}
