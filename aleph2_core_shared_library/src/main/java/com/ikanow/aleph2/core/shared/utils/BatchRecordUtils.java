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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/** A handy centralized supply of implementations of IBatchRecord
 * @author Alex
 */
public class BatchRecordUtils {
	private static final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	/** Simple implementation of IBatchRecord - supports both JSON and content
	 * @author jfreydank
	 */
	public static class BatchRecord implements IBatchRecord, Serializable {
		private static final long serialVersionUID = -1059865997758583525L;
				
		/** Serialize
		 * @param oos
		 * @throws IOException
		 */
		private void writeObject(ObjectOutputStream oos) throws IOException {
			oos.writeUTF(_json.toString());
			if (null != _content) {
				oos.write(_content.toByteArray());
			}			
		}
		/** Deserialize
		 * @param ois
		 * @throws JsonProcessingException
		 * @throws IOException
		 */
		private void readObject(ObjectInputStream ois) throws JsonProcessingException, IOException {
			_json = _mapper.readTree(ois.readUTF());
			int remaining = ois.available();
			if (remaining > 0) {
				_content = new ByteArrayOutputStream();
				byte[] b = new byte[remaining];
				ois.read(b);
				_content.write(b);
			}
		}
		
		/** USer c'tor
		 * @param json
		 * @param content
		 */
		public BatchRecord(final JsonNode json, final ByteArrayOutputStream content) {
			_json = json;
			_content = content;
		}		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord#getJson()
		 */
		public JsonNode getJson() { return _json; }
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord#getContent()
		 */
		public Optional<ByteArrayOutputStream> getContent() { return Optional.ofNullable(_content); }
		
		protected JsonNode _json; 
		protected ByteArrayOutputStream _content;
	}
	
	/** Simple implementation of IBatchRecord - supports JSON only
	 * @author jfreydank
	 */
	public static class JsonBatchRecord implements IBatchRecord, Serializable {
		private static final long serialVersionUID = 4202750376887312365L;
		
		/** Serialize
		 * @param oos
		 * @throws IOException
		 */
		private void writeObject(ObjectOutputStream oos) throws IOException {
			oos.writeUTF(_json.toString());
		}
		/** Deserialize
		 * @param ois
		 * @throws JsonProcessingException
		 * @throws IOException
		 */
		private void readObject(ObjectInputStream ois) throws JsonProcessingException, IOException {
			_json = _mapper.readTree(ois.readUTF());
		}
		
		/** USer c'tor
		 * @param json
		 * @param content
		 */
		public JsonBatchRecord(final JsonNode json) {
			_json = json;
		}		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord#getJson()
		 */
		public JsonNode getJson() { return _json; }
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord#getContent()
		 */
		public Optional<ByteArrayOutputStream> getContent() { return Optional.empty(); }
		
		protected JsonNode _json; 
	}
}
