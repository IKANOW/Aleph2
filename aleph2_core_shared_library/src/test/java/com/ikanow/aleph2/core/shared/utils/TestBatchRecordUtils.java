/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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

package com.ikanow.aleph2.core.shared.utils;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** Test cases
 * @author Alex
 */
public class TestBatchRecordUtils {

	@Test
	public void check_batchRecordIsSerializable() throws IOException {
		
		final ObjectNode test_json = BeanTemplateUtils.configureMapper(Optional.empty()).createObjectNode();
		
		test_json.put("test", "test");
		
		final ByteArrayOutputStream test_out = new ByteArrayOutputStream();
		test_out.write("test2".getBytes());
		
		{
			final IBatchRecord test_case = new BatchRecordUtils.BatchRecord(test_json, null);
			
			final byte[] phase1 = SerializationUtils.serialize((Serializable) test_case);
			
			final IBatchRecord phase2 = (IBatchRecord)SerializationUtils.deserialize(phase1);
			
			assertEquals("{\"test\":\"test\"}", phase2.getJson().toString());
		}		
		{
			final IBatchRecord test_case = new BatchRecordUtils.JsonBatchRecord(test_json);
			
			final byte[] phase1 = SerializationUtils.serialize((Serializable) test_case);
			
			final IBatchRecord phase2 = (IBatchRecord)SerializationUtils.deserialize(phase1);
			
			assertEquals("{\"test\":\"test\"}", phase2.getJson().toString());
		}		
		{
			final IBatchRecord test_case = new BatchRecordUtils.BatchRecord(test_json, test_out);
			
			final byte[] phase1 = SerializationUtils.serialize((Serializable) test_case);
			
			final IBatchRecord phase2 = (IBatchRecord)SerializationUtils.deserialize(phase1);
			
			assertEquals("{\"test\":\"test\"}", phase2.getJson().toString());
			assertEquals("test2", phase2.getContent().get().toString());
		}	
		{
			final IBatchRecord test_case = new BatchRecordUtils.BatchRecord(test_json, test_out);
			final Tuple2<Long, IBatchRecord> test_t2 = Tuples._2T(3L, test_case);
			
			final byte[] phase1 = SerializationUtils.serialize(test_t2);
			@SuppressWarnings("unchecked")
			final Tuple2<Long, IBatchRecord> phase2 = (Tuple2<Long, IBatchRecord>)SerializationUtils.deserialize(phase1);
			
			assertEquals(3L, phase2._1().longValue());
			assertEquals("{\"test\":\"test\"}", phase2._2().getJson().toString());
			assertEquals("test2", phase2._2().getContent().get().toString());
		}
	}
	
}
