package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Optional;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;

public interface IParser {

		// Returns null when done
	Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream);
	default boolean multipleRecordsPerFile(){
		return false;
	}

}
