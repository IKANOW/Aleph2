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
 *******************************************************************************/
package com.ikanow.aleph2.data_import_manager.batch_enrichment.actors;

import java.io.Serializable;

/** THIS CODE IS GETTING MOVED INTO THE DATA ANALYTICS MANAGER TRIGGER LOGIC
 * @author jfreydank
 */
public class BucketEnrichmentMessage implements Serializable{
	private static final long serialVersionUID = 1L;
	private String bucketPathStr;
	public String getBucketPathStr() {
		return bucketPathStr;
	}

	public String getBucketZkPath() {
		return bucketZkPath;
	}


	private String bucketZkPath;
	private String buckeFullName;

	public String getBuckeFullName() {
		return buckeFullName;
	}

	public BucketEnrichmentMessage(String bucketPathStr, String bucketFullName, String bucketZkPath) {
		this.bucketPathStr = bucketPathStr;
		this.buckeFullName = bucketFullName;
		this.bucketZkPath = bucketZkPath;
	}

	@Override
	public String toString() {
		return "bucketPathStr:"+bucketPathStr+"bucketFullName:"+buckeFullName+",bucketZkPath:"+bucketZkPath;
	}
}
