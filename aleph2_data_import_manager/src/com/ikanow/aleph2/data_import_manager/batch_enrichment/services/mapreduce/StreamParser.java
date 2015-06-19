package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class StreamParser implements IParser {

	private static final Logger logger = LogManager.getLogger(StreamParser.class);

	@Override
	public Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream) {
		ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> t3 = null;
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
			t3 = new Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>(currentFileIndex, node, Optional.of(outStream));
		} catch (Exception e) {
			logger.error("JsonParser caught exception",e);
		}
		return t3;
	}

}
