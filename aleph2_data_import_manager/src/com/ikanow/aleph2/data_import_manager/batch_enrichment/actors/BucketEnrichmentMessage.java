/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.batch_enrichment.actors;

import java.io.Serializable;

public class BucketEnrichmentMessage implements Serializable{
	
	/**
	 * 
	 */
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
