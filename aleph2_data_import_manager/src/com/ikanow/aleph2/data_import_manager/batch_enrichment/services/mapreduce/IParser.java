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
package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.InputStream;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;

public interface IParser {

		// Returns null when done
	Tuple2<Long, IBatchRecord> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream);
	default boolean multipleRecordsPerFile(){
		return false;
	}

}
