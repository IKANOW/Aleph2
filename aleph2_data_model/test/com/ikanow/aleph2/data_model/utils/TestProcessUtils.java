package com.ikanow.aleph2.data_model.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Optional;

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
	public void testStopLongRunningProcess() throws FileNotFoundException, IOException, InterruptedException {
		if ( SystemUtils.IS_OS_WINDOWS ) {			
			System.out.println("ProcessUtils do not work on Windows systems (can't get pids)");
			return;
		}
		
		//start a process
		final String root_path = System.getProperty("java.io.tmpdir") + File.separator;
		final String tmp_file_path = createTestScript(getLongRunningProcess());	
		final DataBucketBean bucket = getTestBucket();
		final String application_name = "testing";
		final ProcessBuilder pb = getEnvProcessBuilder(tmp_file_path, root_path);		
		final Tuple2<String, String> launch = ProcessUtils.launchProcess(pb, application_name, bucket, root_path, Optional.empty());
		assertNotNull(launch._1, launch._2);
		
		//check its still running
		assertTrue(ProcessUtils.isProcessRunning(application_name, bucket, root_path));		
		
		//stop the process
		final Tuple2<String, Boolean> stop = ProcessUtils.stopProcess(application_name, bucket, root_path, Optional.empty());
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
		
		final String root_path = System.getProperty("java.io.tmpdir")  + File.separator;
		final DataBucketBean bucket = getTestBucket();
		final String application_name = "testing";
		final Tuple2<String, Boolean> stop_result = ProcessUtils.stopProcess(application_name, bucket, root_path, Optional.empty());
		assertFalse(stop_result._1, stop_result._2);
	}
	
	@Test
	public void testStopDoneProcess() throws FileNotFoundException, IOException, InterruptedException {
		if ( SystemUtils.IS_OS_WINDOWS ) {			
			System.out.println("ProcessUtils do not work on Windows systems (can't get pids)");
			return;
		}
		
		//start a process
		final String root_path = System.getProperty("java.io.tmpdir")  + File.separator;
		final String tmp_file_path = createTestScript(getQuickRunningProcess());			
		final DataBucketBean bucket = getTestBucket();
		final String application_name = "testing";
		final ProcessBuilder pb = getEnvProcessBuilder(tmp_file_path, root_path);		
		final Tuple2<String, String> launch = ProcessUtils.launchProcess(pb, application_name, bucket, root_path, Optional.empty());
		assertNotNull(launch._1, launch._2);
		
		//wait for process to finish (max of 1s)
		for ( int i = 0; i < 10; i++ ) {
			if ( !ProcessUtils.isProcessRunning(application_name, bucket, root_path) ) {
				break;
			} else {
				Thread.sleep(100);
			}
		}
		
		//check its still running
		assertFalse(ProcessUtils.isProcessRunning(application_name, bucket, root_path));		
		
		//stop the process anyways
		final Tuple2<String, Boolean> stop = ProcessUtils.stopProcess(application_name, bucket, root_path, Optional.empty());

		assertTrue(stop._1, stop._2); //stop returns true, but says its already dead
		
		//cleanup
		new File(tmp_file_path).delete();
	}
	
	@Test
	public void testTimeoutLongRunningProcess() throws FileNotFoundException, IOException, InterruptedException {
		if ( SystemUtils.IS_OS_WINDOWS ) {			
			System.out.println("ProcessUtils do not work on Windows systems (can't get pids)");
			return;
		}
		
		//start a process with a timeout of 5s
		final String root_path = System.getProperty("java.io.tmpdir") + File.separator;
		final String tmp_file_path = createTestScript(getLongRunningProcess());	
		final DataBucketBean bucket = getTestBucket();
		final String application_name = "testing";
		final ProcessBuilder pb = getEnvProcessBuilder(tmp_file_path, root_path);		
		final Tuple2<String, String> launch = ProcessUtils.launchProcess(pb, application_name, bucket, root_path, Optional.of(new Tuple2<Long, Integer>(3L, 9)));
		assertNotNull(launch._1, launch._2);
		
		//check its still running
		assertTrue(ProcessUtils.isProcessRunning(application_name, bucket, root_path));		
		
		//wait 5s for process to timeout
		Thread.sleep(5000);
		
		//check the process stopped
		assertFalse("Process should have timed out and died", ProcessUtils.isProcessRunning(application_name, bucket, root_path));		
		
		//cleanup
		new File(tmp_file_path).delete();
	}
	
	@Test
	public void testProcessIgnoreKillSignal() throws FileNotFoundException, IOException {
		if ( SystemUtils.IS_OS_WINDOWS ) {			
			System.out.println("ProcessUtils do not work on Windows systems (can't get pids)");
			return;
		}
		
		//start a process with a timeout of 5s
		final String root_path = System.getProperty("java.io.tmpdir") + File.separator;
		final String tmp_file_path = createTestScript(getIgnoreKillTestScript());	
		final DataBucketBean bucket = getTestBucket();
		final String application_name = "testing";
		final ProcessBuilder pb = getEnvProcessBuilder(tmp_file_path, root_path);		
		final Tuple2<String, String> launch = ProcessUtils.launchProcess(pb, application_name, bucket, root_path, Optional.empty());
		assertNotNull(launch._1, launch._2);
		
		//check its still running
		assertTrue(ProcessUtils.isProcessRunning(application_name, bucket, root_path));		
		
		//try to stop process with kill -2, it should have to force kill it with kill -9
		final Tuple2<String, Boolean> stop = ProcessUtils.stopProcess(application_name, bucket, root_path, Optional.of(2));

		assertTrue(stop._1, stop._2); //stop returns true		
		
		//check the process stopped
		assertFalse("Process should have timed out and died", ProcessUtils.isProcessRunning(application_name, bucket, root_path));		
		
		//cleanup
		new File(tmp_file_path).delete();
	}	

	private static String createTestScript(final String script) throws FileNotFoundException, IOException {		
		new File(System.getProperty("java.io.tmpdir")  + File.separator  + "test_pid_scripts" + File.separator).mkdir();
		final String file_path = System.getProperty("java.io.tmpdir")  + File.separator  + "test_pid_scripts" + File.separator  + UuidUtils.get().getRandomUuid() + ".sh";
		final File file = new File(file_path);
		IOUtils.write(script, new FileOutputStream(file));
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
//				.append("trap \"exit 47\" SIGTERM\n")
			.append("while [ : ]\n")
			.append("do\n")
			.append("   sleep 1\n")
			.append("done\n")
		.toString();
	}
	
	private String getIgnoreKillTestScript() {
		return new StringBuilder()
				.append("trap \"echo caught 2, not dieing\" 2\n\n" )
				.append(getLongRunningProcess())
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
		//root path has to exist if it doesn't already
		new File(root_path + File.separator + "run").mkdir();
		ProcessBuilder pb = new ProcessBuilder("sh",  script_file_loc );				
		pb.directory(new File(root_path + File.separator  + "run" )).redirectErrorStream(true);
		pb.environment().put("JAVA_OPTS", "");
		return pb;
	}

}
