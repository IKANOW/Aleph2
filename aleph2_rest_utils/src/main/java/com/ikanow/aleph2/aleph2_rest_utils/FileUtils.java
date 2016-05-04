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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;

/**
 * @author Burch
 *
 */
public class FileUtils {
	public static int DEFAULT_BUFFER_SIZE = 65536; //TODO pick some sensible buffer size
	
	public static void writeFile(final FileContext fileContext, final InputStream input, final String path) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IOException {
		final Path p = new Path(path);		
		try (FSDataOutputStream outer = fileContext.create(p, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 
				org.apache.hadoop.fs.Options.CreateOpts.createParent())) {			
			IOUtils.copyLarge(input, outer, new byte[DEFAULT_BUFFER_SIZE]);			
		}
	}
}
