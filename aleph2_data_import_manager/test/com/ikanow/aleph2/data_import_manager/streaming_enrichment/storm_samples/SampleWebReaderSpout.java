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
package com.ikanow.aleph2.data_import_manager.streaming_enrichment.storm_samples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

import jline.internal.InputStreamReader;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Tries to open a local file and emits tuples per line
 * of the file.
 * 
 * @author Burch
 *
 */
public class SampleWebReaderSpout extends BaseRichSpout {

	private static final long serialVersionUID = 2777840612433168585L;
	private SpoutOutputCollector _collector;
	private BufferedReader reader;
	private String file_location;

	public SampleWebReaderSpout(String source_url) {
		file_location = source_url;
	}

	@Override
	public void nextTuple() {
		//send lines in the log file until we run out
		if ( reader != null ) {
			try {
				String line = reader.readLine();
				if ( line == null )
					reader = null;
				else
					_collector.emit(new Values(line));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		
		//open web file sample
		try {
			InputStream is = new URL(file_location).openStream();
			reader = new BufferedReader(new InputStreamReader(is));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log_entry"));
	}

}
