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
package com.ikanow.aleph2.data_import_manager.utils;


import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/** 
 * Utility class working on FileContext.
 * @author jfreydank
 *
 */
public class DirUtils {
	private static final Logger logger = LogManager.getLogger(DirUtils.class);

	/**
	 * This method returns the path to the first subdirectory matching the subDirectoryName parameter or null if not found.
	 */
	 public static Path findOneSubdirectory(FileContext fileContext, Path start, String subDirectoryName) {	 
		Path p = null;
		try {
			logger.debug("findOneSubdirectory :"+start.toString());
			FileStatus[] statuss = fileContext.util().listStatus(start);
			for (int i = 0; i < statuss.length; i++) {
				FileStatus dir = statuss[i];
				logger.debug("FileStatus:" + statuss[i].getPath().toString());
				if(dir.isDirectory()){
					if(dir.getPath().getName().contains(subDirectoryName)){
						logger.debug("findOneSubdirectory match:"+dir.getPath().getName());
						return dir.getPath();
					}else{
						p = findOneSubdirectory(fileContext, dir.getPath(),  subDirectoryName);
						if(p!=null){
							return p;							
						}
					}
				}
			}

		} catch (Exception e) {
			logger.error("findOneSubdirectory Caught Exception", e);
		}

		return p;
	}

	public static void createDirectory(FileContext fileContext,String pathString) {
		if(fileContext!=null && pathString !=null){
			try {
				Path dir = new Path(pathString);
				if(!fileContext.util().exists(dir)){
					fileContext.mkdir(dir, FileContext.DIR_DEFAULT_PERM, true);
				}
			} catch (Exception e) {
				logger.error("createFolderStructure Caught Exception", e);
			}
		}
		
	}

	public static void findAllSubdirectories(List<Path> allPaths, FileContext fileContext, Path start, String subDirectoryName,boolean includeMatched) {		
		try {
			logger.debug("findAllSubdirectories :"+start.toString());
			FileStatus[] statuss = fileContext.util().listStatus(start);
			for (int i = 0; i < statuss.length; i++) {
				FileStatus dir = statuss[i];
				logger.debug("FileStatus:" + statuss[i].getPath().toString());
				if(dir.isDirectory()){
					if(dir.getPath().getName().contains(subDirectoryName)){
						logger.debug("findOneSubdirectory match:"+dir.getPath().getName());
						if(includeMatched){
							allPaths.add(dir.getPath());
						}else{
							allPaths.add(dir.getPath().getParent());
						}
					}else{
						findAllSubdirectories(allPaths, fileContext, dir.getPath(),  subDirectoryName,includeMatched);
						/*if(paths!=null){
							allPaths.addAll(paths);							
						}*/
					}
				}
			}

		} catch (Exception e) {
			logger.error("findAllSubdirectories Caught Exception", e);
		}		
	}

	public static void createUTF8File(FileContext fileContext,String fileNameString, StringBuffer sb) {
		if(fileContext!=null && fileNameString !=null){
			try {
				Path f = new Path(fileNameString);
					FSDataOutputStream out = fileContext.create(f, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
					//Read from input stream and write to output stream until EOF.
					Charset charset = StandardCharsets.UTF_8;
					CharsetEncoder encoder = charset.newEncoder();

					// No allocation performed, just wraps the StringBuilder.
					CharBuffer buffer = CharBuffer.wrap(sb.toString());

					byte[] bytes = encoder.encode(buffer).array();
    				  out.write(bytes);
					  out.close();
					
			} catch (Exception e) {
				logger.error("createFolderStructure Caught Exception", e);
			}
		}
		
	}

}
