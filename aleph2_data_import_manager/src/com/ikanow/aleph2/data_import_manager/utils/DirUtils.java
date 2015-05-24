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

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
/** 
 * Utility class working on FileContext.
 * @author jfreydank
 *
 */
public class DirUtils {
	private static final Logger logger = Logger.getLogger(DirUtils.class);

	/**
	 * This method returns the path to the first subdirectory matching the subDirectoryName parameter or null if not found.
	 */
	 public static Path findOneSubdirectory(FileContext fileContext, Path start, String subDirectoryName) {	 
		Path p = null;
		try {
			logger.debug("findOneSubdirectory:"+start);
			FileStatus[] statuss = fileContext.util().listStatus(start);
			logger.debug(statuss.length);
			for (int i = 0; i < statuss.length; i++) {
				FileStatus dir = statuss[i];
				if(dir.isDirectory()){
					if(dir.getPath().getName().contains(subDirectoryName)){
						logger.debug("findOneSubdirectory match:"+dir.getPath().getName());
						return dir.getPath();
					}else{
					    return findOneSubdirectory(fileContext, dir.getPath(),  subDirectoryName);
					}
				}
				logger.debug("FileStatus:" + statuss[i].toString());
			}

		} catch (Exception e) {
			logger.error("findOneSubdirectory Caught Exception", e);
		}

		return p;
	}

}