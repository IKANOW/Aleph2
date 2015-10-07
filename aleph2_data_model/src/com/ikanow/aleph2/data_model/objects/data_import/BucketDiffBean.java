/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.ikanow.aleph2.data_model.objects.data_import;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/** Placeholder object for representing the differences in a bucket update, to help
 *  harvest/enrichment authors decide how best to handle them
 * @author acp
 *
 */
public class BucketDiffBean implements Serializable {
	private static final long serialVersionUID = -5936448294402600187L;

	protected BucketDiffBean() {}
	
	/**
	 * Returns a diff of every top level field in DataBucketBean
	 * @return
	 */
	public Map<String, Boolean> diffs(){ return this.diffs; }
	/**
	 * Returns a set of shared libraries that have been modified.
	 * @return
	 */
	public Set<String> lib_diffs() { return this.lib_diffs; }
	
	private Map<String, Boolean> diffs;
	private Set<String> lib_diffs;
}
