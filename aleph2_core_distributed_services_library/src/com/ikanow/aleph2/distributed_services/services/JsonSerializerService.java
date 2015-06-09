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
package com.ikanow.aleph2.distributed_services.services;

import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

import akka.serialization.JSerializer;

/** Jackson based serialization library for messages that include Aleph2 beans
 * @author Alex
 */
public class JsonSerializerService extends JSerializer {

	protected final ObjectMapper _mapper;
	
	public JsonSerializerService() {
		_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	}
	
	@Override
	public int identifier() {
		return 0x41454B58; // (int too short for Aleph2 :( )
	}

	@Override
	public boolean includeManifest() {
		return true;
	}

	@Override
	public byte[] toBinary(Object arg0) {
		/**/
		System.out.println("toBinary!");		
		try {
			return _mapper.writeValueAsBytes(arg0);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object fromBinaryJava(byte[] arg0, Class<?> arg1) {
		/**/
		System.out.println("fromBinary! " + (arg1 == null ? "null" : arg1.toString()));
		try {
			return _mapper.readValue(arg0, arg1);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
}
