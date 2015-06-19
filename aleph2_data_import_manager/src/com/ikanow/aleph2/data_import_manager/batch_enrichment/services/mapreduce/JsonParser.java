package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class JsonParser implements IParser {
	private static final Logger logger = LogManager.getLogger(JsonParser.class);

	@Override
	public Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream) {
		ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> t3 = null;
		try {
			JsonNode node = object_mapper.readTree(inStream);
			t3 = new Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>(currentFileIndex, node, Optional.empty());
		} catch (Exception e) {
			logger.error("JsonParser caught exception",e);
		}
		return t3;
	}

}
