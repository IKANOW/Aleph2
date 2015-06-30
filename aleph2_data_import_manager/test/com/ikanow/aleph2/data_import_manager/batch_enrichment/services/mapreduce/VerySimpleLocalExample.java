package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import java.util.Arrays;

public class VerySimpleLocalExample {

	@Test
	public void testSetup() {		
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}
	}
	
	@SuppressWarnings({ "deprecation", "unchecked", "rawtypes" })
	@Test
	public void test_localHadoopLaunch() throws IOException, IllegalStateException, ClassNotFoundException, InterruptedException {
				
		// 0) Setup the temp dir 
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		//final Path tmp_path = FileContext.getLocalFSFileContext().makeQualified(new Path(temp_dir));
		final Path tmp_path2 = FileContext.getLocalFSFileContext().makeQualified(new Path(temp_dir + "/tmp_output"));
		try {
			FileContext.getLocalFSFileContext().delete(tmp_path2, true);
		}
		catch (Exception e) {} // (just doesn't exist yet)
		
		// 1) Setup config with local mode
		final Configuration config = new Configuration();		
		config.setBoolean("mapred.used.genericoptionsparser", true); // (just stops an annoying warning from appearing)
		config.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		config.set("mapred.job.tracker", "local");
		config.set("fs.defaultFS", "local");
		config.unset("mapreduce.framework.name");
		
		// If running locally, turn "snappy" off - tomcat isn't pointing its native library path in the right place
		config.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
		
		// 2) Build job and do more setup using the Job API
		//TODO: not sure why this is deprecated, it doesn't seem to be in v1? We do need to move to JobConf at some point, but I ran into some 
		// issues when trying to do everything I needed to for V1, so seems expedient to start here and migrate away
		final Job hj = new Job( config ); // (NOTE: from here, changes to config are ignored)
		
		// Input format:
		//TOOD: fails because of guava issue, looks like we'll need to move to 2.7 and check it works with 2.5.x server?
		//TextInputFormat.addInputPath(hj, tmp_path);
		//hj.setInputFormatClass((Class<? extends InputFormat>) Class.forName ("org.apache.hadoop.mapreduce.lib.input.TextInputFormat"));
		hj.setInputFormatClass(TestInputFormat.class);

		// Output format:
		hj.setOutputFormatClass((Class<? extends OutputFormat>) Class.forName ("org.apache.hadoop.mapreduce.lib.output.TextOutputFormat"));
		TextOutputFormat.setOutputPath(hj, tmp_path2);
			
		// Mapper etc (combiner/reducer are similar)
		hj.setMapperClass(TestMapper.class);
		hj.setOutputKeyClass(Text.class); 
		hj.setOutputValueClass(Text.class);
		hj.setNumReduceTasks(0); // (disable reducer for now)
		
		hj.setJar("test");
		
		try {
			hj.submit();
		}
		catch (UnsatisfiedLinkError e) {
			throw new RuntimeException("This is a windows/hadoop compatibility problem - adding the hadoop-commons in the misc_test_assets subdirectory to the top of the classpath should resolve it (and does in V1), though I haven't yet made that work with Aleph2", e);
		}
		//hj.getJobID().toString();
		while (!hj.isComplete()) {
			Thread.sleep(1000);
		}
		assertTrue("Finished successfully", hj.isSuccessful());
	}
	////////////////////////////////////////////////////////
	
	public static class TestMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		public void map( Text key, Text value, Context context ) throws IOException, InterruptedException
		{
			System.out.println("MAP!!");
		}
	}
	
	public static class TestRecordReader extends RecordReader<Text, Text> {

		boolean done = false;
		
		@Override
		public void close() throws IOException {
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return new Text("test");
		}

		@Override
		public Text getCurrentValue() throws IOException,
				InterruptedException {
			return new Text("test");
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!done) {
				done = true;
				return true;
			}
			else {
				return false;
			}
		}
	}
	public static class TestInputSplit extends InputSplit implements Writable {

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return Arrays.asList("").toArray(new String[0]);
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
		}
		
	}
	public static class TestInputFormat extends InputFormat<Text, Text> {

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
				TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			return new TestRecordReader();
		}

		@Override
		public List<InputSplit> getSplits(JobContext arg0) throws IOException,
				InterruptedException {
			return Arrays.asList(new TestInputSplit());
		}
		
	}
	
}
