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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class StreamParser implements IParser {

	private static final Logger logger = LogManager.getLogger(StreamParser.class);
	
	@Override
	public Tuple2<Long, IBatchRecord> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream) {
		logger.debug("StreamParser.getNextRecord");

		ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		Tuple2<Long, IBatchRecord> t2 = null;
		try {
		   JsonNode node = mapper.createObjectNode(); 
		   ((ObjectNode) node).put("fileName", fileName);
		   // create output stream
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
	        int readedBytes;
	        byte[] buf = new byte[1024];
	        while ((readedBytes = inStream.read(buf)) > 0)
	        {
	            outStream.write(buf, 0, readedBytes);
	        }
	        outStream.close();			    	
			t2 = new Tuple2<Long, IBatchRecord>(currentFileIndex, new BeFileInputReader.BatchRecord(node, outStream));
		} catch (Exception e) {
			logger.error("JsonParser caught exception",e);
		}
		return t2;
	}

}
