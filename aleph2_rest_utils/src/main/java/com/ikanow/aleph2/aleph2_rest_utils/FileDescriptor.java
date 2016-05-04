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
package com.ikanow.aleph2.aleph2_rest_utils;

import java.io.InputStream;

/**
 * @author Burch
 *
 */
public class FileDescriptor {
	private final InputStream input_stream;
	private final String file_name;
	
	public FileDescriptor(final InputStream input_stream, final String file_name) {
		this.input_stream = input_stream;
		this.file_name = file_name;
	}
	
	public InputStream input_stream() { return this.input_stream; }
	public String file_name() { return this.file_name; }
}
