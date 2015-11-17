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
 *******************************************************************************/
package com.ikanow.aleph2.core.shared.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;













import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.io.ByteStreams;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;


/**
 * Utility class to merge any number of jar files.
 * 
 * @author Burch
 *
 */
public class JarBuilderUtil {
	private static final Logger logger = LogManager.getLogger();
	private static final String DEFAULT_JAR_NAME_PREFIX = "aleph2_storm_jar_";
	
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
        mergeJars(jars_to_merge, output_path, new HashSet<String>());
	}
	
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
	 * Does not merge in any dir paths with names matching something in "dir_names_to_not_merge"
	 * 
	 * @param jars_to_merge
	 * @param output_path
	 * @throws IOException
	 */
	public static void mergeJars(Collection<String> jars_to_merge, String output_path, Set<String> dir_names_to_not_merge) throws IOException {
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
	                	//logger.debug("copy: " + e.getName());
	                	if ( !shouldExclude(e.getName(), dir_names_to_not_merge) ) {
		                    outputZip.putNextEntry(e);
		                    if (!e.isDirectory() ) {
		                    	ByteStreams.copy(currentZip.getInputStream(e), outputZip);
		                    }	    
	                	} else {
	                		//logger.debug("skipping: " + e.getName() + " because it's in our do not merge list");
	                	}
	                } catch (ZipException ex) {
	                	//duplicate file, just skip because we don't allow overwrites	
	                	//logger.debug("\tcouldn't overwrite: " + e.getName());
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
	 * Check if the current file_path is starts with any of our excluded directories
	 * 
	 * @param file_path
	 * @param dir_names_to_not_merge 
	 * @return
	 */
    private static boolean shouldExclude(String file_path, Set<String> dir_names_to_not_merge) {
		for ( String dir_to_not_merge : dir_names_to_not_merge ) {
			if ( file_path.startsWith(dir_to_not_merge) )
				return true;
		}
		return false;
	}
    
    /**
	 * Hashes the jar names together to create a unique hash, then returns a filepath with that
	 * hash set to tmpdir/{hash}.jar
	 * 
	 * The hash is only guaranteed to be the same if the file order in jars_to_merge is the same,
	 * this is necessary because of the way we merge the files together to create the jar.
	 * 
	 * @param jars_to_merge
	 * @return
	 */
	public static String getHashedJarName(final Collection<String> jars_to_merge, String output_folder) {				
		final String hash = String.valueOf( Objects.hash(jars_to_merge));
		final String output_location = output_folder + File.separator + DEFAULT_JAR_NAME_PREFIX + hash + ".jar";
		return output_location;
	}
	
	/**
	 * Returns the date of the most recently updated file in the list
	 * 
	 * @param jars_to_merge
	 * @return
	 */
	public static Date getMostRecentlyUpdatedFile(final Collection<String> jars_to_merge) {
		long most_recent_update = jars_to_merge.stream().map(jar_path -> {			
			File file = new File(jar_path);
			long last_modified = file.lastModified();
			logger.debug("File: " + jar_path + " was last modified on: " + new Date(last_modified) + " ms: " + last_modified);			
			return last_modified;			
		}).reduce(0L, (a, b) -> {
			if ( a >= b )
				return a;
			else
				return b;
		});
		return new Date(most_recent_update);
	}

	/**
	 * Sets the given files modified time to now.
	 * @param hashed_jar_name
	 */
	public static void updateJarModifiedTime(String hashed_jar_name) {
		File file = new File(hashed_jar_name);
		if ( file.exists() ) {
			file.setLastModified(System.currentTimeMillis());
		}
	}
}
