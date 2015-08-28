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
package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;

public class BeFileInputFormat extends CombineFileInputFormat<String, Tuple2<Long, IBatchRecord>> {

	
	private static final Logger logger = LogManager.getLogger(BeFileInputFormat.class);

	public BeFileInputFormat(){
		super();
		logger.debug("BeFileInputFormat.constructor");
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		logger.debug("BeFileInputFormat.isSplitable");
		return false;
	}

	@Override
	public RecordReader<String, Tuple2<Long, IBatchRecord>> createRecordReader(InputSplit inputSplit,	TaskAttemptContext context) throws IOException {
		logger.debug("BeFileInputFormat.createRecordReader");
		BeFileInputReader reader = new BeFileInputReader();
		try {
			reader.initialize(inputSplit, context);
		} 
		catch (InterruptedException e) {
			throw new IOException(e);
		}
		return reader;
	} // createRecordReader
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		logger.debug("BeFileInputFormat.getSplits");
		List<InputSplit> tmp = null;
		try {
			
			tmp = super.getSplits(context);
		} catch (Throwable t) {
			logger.error(t);
		}
		
		logger.debug("BeFileInputFormat.getSplits: " +((tmp!=null)? tmp.size():"null"));
		return tmp;
	}
}
