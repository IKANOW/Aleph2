package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;

public class StreamParser implements IParser {

	@Override
	public Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
