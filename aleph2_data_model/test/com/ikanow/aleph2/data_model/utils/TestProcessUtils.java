package com.ikanow.aleph2.data_model.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SystemUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

public class TestProcessUtils {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws FileNotFoundException, IOException, InterruptedException {
		if ( SystemUtils.IS_OS_WINDOWS ) {			
			System.out.println("ProcessUtils do not work on Windows systems (can't get pids)");
			return;
		}
		
		//start a process
		final String root_path = System.getProperty("java.io.tmpdir");
		final String tmp_file_path = createTestScript(getLongRunningProcess());	
		System.out.println(tmp_file_path);
		//wait a second for script file to get written?
//		Thread.sleep(1000);
		final DataBucketBean bucket = getTestBucket();
		final String application_name = "testing";
		final ProcessBuilder pb = getEnvProcessBuilder(tmp_file_path, root_path);		
		final Tuple2<String, String> launch = ProcessUtils.launchProcess(pb, application_name, bucket, root_path);
		assertNotNull(launch._1, launch._2);
		
		//check its still running
		assertTrue(ProcessUtils.isProcessRunning(application_name, bucket, root_path));		
		
		//stop the process
		final Tuple2<String, Boolean> stop = ProcessUtils.stopProcess(application_name, bucket, root_path);
		assertTrue(stop._1, stop._2);
		
		//cleanup
		new File(tmp_file_path).delete();
	}
	
	@Test
	public void testStopNonExistantProcess() {
		if ( SystemUtils.IS_OS_WINDOWS ) {			
			System.out.println("ProcessUtils do not work on Windows systems (can't get pids)");
			return;
		}
		
		final String root_path = System.getProperty("java.io.tmpdir");
		final DataBucketBean bucket = getTestBucket();
		final String application_name = "testing";
		final Tuple2<String, Boolean> stop_result = ProcessUtils.stopProcess(application_name, bucket, root_path);
		assertFalse(stop_result._1, stop_result._2);
	}
	
	@Test
	public void testStopDoneProcess() throws FileNotFoundException, IOException, InterruptedException {
		if ( SystemUtils.IS_OS_WINDOWS ) {			
			System.out.println("ProcessUtils do not work on Windows systems (can't get pids)");
			return;
		}
		
		//start a process
		final String root_path = System.getProperty("java.io.tmpdir");
		final String tmp_file_path = createTestScript(getQuickRunningProcess());	
		System.out.println(tmp_file_path);
		//wait a second for script file to get written?
//		Thread.sleep(1000);
		final DataBucketBean bucket = getTestBucket();
		final String application_name = "testing";
		final ProcessBuilder pb = getEnvProcessBuilder(tmp_file_path, root_path);		
		final Tuple2<String, String> launch = ProcessUtils.launchProcess(pb, application_name, bucket, root_path);
		assertNotNull(launch._1, launch._2);
		
		//check its still running
		assertFalse(ProcessUtils.isProcessRunning(application_name, bucket, root_path));		
		
		//stop the process anyways
		final Tuple2<String, Boolean> stop = ProcessUtils.stopProcess(application_name, bucket, root_path);
		assertTrue(stop._1, stop._2); //stop returns true, but says its already dead
		
		//cleanup
		new File(tmp_file_path).delete();
	}
	
	private static String createTestScript(final String script) throws FileNotFoundException, IOException {
		final String file_path = System.getProperty("java.io.tmpdir") + UuidUtils.get().getRandomUuid() + ".sh";
		IOUtils.write(script, new FileOutputStream(new File(file_path)));
		return file_path;
	}
	
	private static DataBucketBean getTestBucket() {
		return BeanTemplateUtils.build(DataBucketBean.class)
			.with(DataBucketBean::_id, UuidUtils.get().getRandomUuid())
			.done().get();
	}
	
	private static String getLongRunningProcess() {
		//assume bash scripts work
		//this script will loop forever, doing nothing, it's utterly useless
		return new StringBuilder()
			.append("while [ : ]\n")
			.append("do\n")
			.append("   sleep 1\n")
			.append("done\n")
		.toString();
	}
	
	private static String getQuickRunningProcess() {
		//assume bash scripts work
		//this script will just print out 'done' then quit
		return new StringBuilder()
			.append("echo 'done'\n")
		.toString();
	}
	
	private static ProcessBuilder getEnvProcessBuilder(final String script_file_loc, final String root_path) {		
		//linux process
		ProcessBuilder pb = new ProcessBuilder("sh \""+ script_file_loc +"\"");
		pb.directory(new File(root_path + File.separator + "run")).redirectErrorStream(true);
		pb.environment().put("JAVA_OPTS", "");
		return pb;
	}

}
