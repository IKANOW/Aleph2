/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
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
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;


/**
 * Utility class to merge any number of jar files.
 * 
 * @author Burch
 *
 */
public class JarBuilderUtil {
	private static final byte[] BUFFER = new byte[4096 * 1024];
	private static final Logger logger = LogManager.getLogger();
	
	/**
	 * Combines all jars into an uber final jar, the final jar will be sent to
	 * output_path.  Jars are merged such that the first jar in jars_to_merge is
	 * the main jar, and every subsequent jar is merged with it, the first instance
	 * of a file will not every be overwrote e.g.:
	 * jar1 contains /com/somedir/fileA.class
	 * jar2 contains /com/somedir/fileA.class
	 * mergeJars([jar1,jar2],output.jar) will result in a jar at output.jar with
	 * the FileA.class from jar1, because it was earlier in the list.
	 * 
	 * @param jars_to_merge
	 * @param output_path
	 * @throws IOException
	 */
	public static void mergeJars(List<String> jars_to_merge, String output_path) throws IOException {
        ZipOutputStream outputZip = new ZipOutputStream(new FileOutputStream(output_path));
        //loop over the zips, the first entry to write a file to zip has precendence (can't be overwritten)        
        jars_to_merge.stream().forEach(zip_path -> {
        	
        	logger.info("copying zip: " + zip_path + " into " + output_path);
        	try {
	        	ZipFile currentZip = new ZipFile(zip_path);
	        	Enumeration<? extends ZipEntry> entries = currentZip.entries();
	            while (entries.hasMoreElements()) {
	                ZipEntry e = entries.nextElement();
	                try {
	                	logger.debug("copy: " + e.getName());
	                    outputZip.putNextEntry(e);
	                    if (!e.isDirectory() ) {
	                        copy(currentZip.getInputStream(e), outputZip);
	                    }	                    
	                } catch (ZipException ex) {
	                	//duplicate file, just skip because we don't allow overwrites	
	                	logger.debug("\tcouldn't overwrite: " + e.getName());
	                } finally {
	                	outputZip.closeEntry();
	                }
	            }
	            currentZip.close();
        	} catch (IOException ex) {
        		logger.error(ErrorUtils.getLongForm("Error during merging zips {0}", ex));
        	}
        });

        // close
        outputZip.close();
        logger.info("merging jars completed");
	}
	
    /**
     * Helper function to copy input stream into output stream
     * 
     * @param input
     * @param output
     * @throws IOException
     */
    public static void copy(InputStream input, OutputStream output) throws IOException {
        int bytesRead;
        while ((bytesRead = input.read(BUFFER))!= -1) {
            output.write(BUFFER, 0, bytesRead);
        }
    }
}
