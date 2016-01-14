package com.ikanow.aleph2.data_model.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;
//import scala.collection.immutable.Stream;



import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

public class ProcessUtils {
	private static final Logger logger = LogManager.getLogger();
	private static final String PID_PREFIX = "/proc/";
	private static final String PID_MANAGER_DIR_NAME = "pid_manager" + File.separator;
	
	
	/** Most applications will use <aleph2_home>/run as the the location to store their process information
	 */
	public static final String DEFAULT_RUN_PATH_SUFFIX = "/run/";

	/**
	 * Starts the given process by calling process_builder.start();
	 * Records the started processes pid and start date.
	 *  
	 * @param process_builder
	 * @throws IOException 
	 * @return returns any error in _1(), the pid in _2()
	 */
	public static Tuple2<String, String> launchProcess(final ProcessBuilder process_builder, final String application_name, final DataBucketBean bucket, final String aleph_root_path, final Optional<Tuple2<Long, Integer>> timeout_kill) {
		try {
			if ( timeout_kill.isPresent() ) {
				final Stream<String> timeout_stream = Stream.of("timeout","-s",timeout_kill.get()._2.toString(), timeout_kill.get()._1.toString());
				process_builder.command(Stream.concat(timeout_stream, process_builder.command().stream()).collect(Collectors.toList()));
			}
			
			//starts the process, get pid back
			logger.debug("Starting process: " + process_builder.command().toString());
			final Process px = process_builder.start();
			String err = null;
			String pid = null;
			if (!px.isAlive()) {
				err = "Unknown error: " + px.exitValue() + ": " + 
						process_builder.command().stream().collect(Collectors.joining(" "));
					// (since not capturing output)
			}
			else {
				pid = getPid(px);
				//get the date on the pid file from /proc/<pid>
				final long date = getDateOfPid(pid);
				//record pid=date to aleph_root_path/pid_manager/bucket._id/application_name
				storePid(application_name, bucket, aleph_root_path, pid, date);
			}
			return Tuples._2T(err, pid);
						
		} catch (Throwable t) {
			return Tuples._2T(ErrorUtils.getLongForm("{0}", t), null);
		}
	}
	
	/**
	 * Switched this to be a 2 step process:
	 * 1. kill any children whose parent is this process (do they need to kill their children?)
	 * 
	 * Attempts to stop the given process if it is still currently running
	 * Will send a 15 if no kill_signal is given.  If the original kill_signal fails to
	 * stop the job w/in 5 seconds, sends a kill -9 to force the kill.
	 * Throw an exception if we fail to stop?
	 * @return Tuple2 _1 for message, _2 for success
	 */
	public static Tuple2<String,Boolean> stopProcess(final String application_name, final DataBucketBean bucket, final String aleph_root_path, final Optional<Integer> kill_signal) {						
		try {
			//gets process pid/date
			final Tuple2<String, Long> pid_date = getStoredPid(application_name, bucket, aleph_root_path);
			if ( pid_date._1 != null ) {
//				checks if that pid still exists and has the same date
				if (!isRunning(pid_date._1, pid_date._2)) {
					return Tuples._2T("(process " + pid_date._1 + " already deleted)", true);
				}
				return Tuples._2T("tried to  kill pid and children", killProcessAndChildren(pid_date._1, kill_signal));
		} else {
				return Tuples._2T("Couldn't find a stored entry for the given application/bucket", false);
			}
		} catch (Throwable t) {//(do nothing)
			return Tuples._2T("Kill failed: " + ErrorUtils.getLongForm("{0}", t), false);
		}
	}

	/**
	 * Finds all a processes children, kills the process then recursively calls this on the children.
	 * 
	 * @param pid
	 * @param kill_signal
	 * @return
	 * @throws IOException 
	 */
	private static boolean killProcessAndChildren(final String pid, final Optional<Integer> kill_signal) throws IOException {
		//first find any children via pgrep -P pid
		final ProcessBuilder pb_children = new ProcessBuilder("pgrep","-P", pid.toString());
		final BufferedReader br = new BufferedReader(new InputStreamReader( pb_children.start().getInputStream()));
		final Set<String> child_pids = new HashSet<String>();
		String line;
		while ( (line=br.readLine()) != null) {
			child_pids.add(line);
		}
		logger.debug("children of pid: " + pid + " are: " + child_pids.toString());
		//kill pid w/ signal
		killProcess(pid, kill_signal);
		
		//if still alive kill with -9, give it 5s to try and die the old way
		for ( int i = 0; i < 5; i++ ) {
			try { Thread.sleep(1000L); } catch (Exception e) {}
			if ( !isProcessRunning(pid) ) {			
				break;
			}
		}
		if ( isProcessRunning(pid) ) {			
			killProcess(pid, Optional.of(9));
		}
		
		//call this on all children
		return !child_pids.stream().filter(child_pid -> {
			try {
				return killProcessAndChildren(child_pid, kill_signal);
			}catch (Exception ex) {
				return false;
			}
			}).anyMatch(result->false);		
	}
	
	private static boolean killProcess(final String pid, final Optional<Integer> kill_signal) throws IOException {
//		kill -15 the process, wait a few cycles to let it die				
		final ProcessBuilder pb = new ProcessBuilder(Arrays.asList("kill", "-" + kill_signal.orElse(15), pid));
		logger.debug("trying to kill -"+kill_signal.orElse(15)+" pid: " + pid);
		final Process px = pb.start();
		for (int i = 0; i < 5; ++i) {
			try { Thread.sleep(1000L); } catch (Exception e) {}
			if ( !isProcessRunning(pid)) {		
				break;					
			}
		}
		if (!isProcessRunning(pid)) {
			return 0 == px.exitValue();
		} else {
			//we are still alive, so send a harder kill signal if we haven't already sent a 9
			if ( kill_signal.isPresent() && kill_signal.get() == 9 ) {
				return false;
			} else {
				logger.debug("Timed out trying to kill: " + pid + " sending kill -9 to force kill");
				return killProcess(pid, Optional.of(9));				
			}
			
							
		}
	}

	/**
	 * Checks if the given process is currently running
	 * @return true if the process is running, false otherwise
	 */
	public static boolean isProcessRunning(final String application_name, final DataBucketBean bucket, final String aleph_root_path) {
		try {
			final Tuple2<String, Long> pid_date = getStoredPid(application_name, bucket, aleph_root_path);
			return isRunning(pid_date._1, pid_date._2);
		} catch (Throwable t) {			
			return false;
		}
	}
	
	private static boolean isProcessRunning(final String pid) {
		final File pid_file = new File("/proc/" + pid);
		return pid_file.exists();		
	}
	
	
	/**
	 * Writes pid=date out to /app/aleph2/pid_manager/bucket._id/application_name
	 * 
	 * @param application_name
	 * @param bucket
	 * @param aleph_root_path
	 * @param pid
	 * @param date
	 * @throws IOException 
	 */
	private static void storePid(final String application_name, final DataBucketBean bucket, final String aleph_root_path, final String pid, final long date) throws IOException {						
		final File file = new File(aleph_root_path + PID_MANAGER_DIR_NAME + bucket._id() + File.separator + application_name);
		file.getParentFile().mkdirs();
		if ( file.exists() )
			file.delete();	
		file.createNewFile();
		final PrintWriter pw = new PrintWriter(file);
		pw.print(pid + "=" + date);
		pw.close();
	}

	/**
	 * Returns back the tuple <pid, date> stored for the given args
	 * 
	 * @param px
	 * @return
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	private static Tuple2<String, Long> getStoredPid(final String application_name, final DataBucketBean bucket, final String aleph_root_path) throws FileNotFoundException, IOException {		
		final File file = new File(aleph_root_path + PID_MANAGER_DIR_NAME + bucket._id() + File.separator + application_name);
		if ( file.exists() ) {
			final String pid_str = IOUtils.toString(new FileInputStream(file), "UTF-8");
			final String[] splits = pid_str.split("=");
			return new Tuple2<String, Long>(splits[0], Long.parseLong(splits[1]));
		}
		return new Tuple2<String, Long>(null, 0L);
	}
	
	/**
	 * Returns the pid for the given process
	 * 
	 * @param px
	 * @return
	 */
	private static String getPid(Process px) {
		try {
	        final Class<?> ProcessImpl = px.getClass();
	        final Field field = ProcessImpl.getDeclaredField("pid");
	        field.setAccessible(true);
	        return Integer.toString(field.getInt(px));
	    } 
	    catch (Throwable t) {
	        return "unknown";
	    }	
	}
	
	/**
	 * Looks in /proc/<pid> and pulls the date of the file
	 * 
	 * @param pid
	 * @return
	 */
	private static long getDateOfPid(final String pid) {
		final File file = new File(PID_PREFIX + pid);
		return file.lastModified();		
	}
	
	/**
	 * Checks if a process is still running with the given pid and started
	 * at the given date
	 * 
	 * @param pid
	 * @param date
	 * @return
	 */
	private static boolean isRunning(String pid, Long date) {
		final File pid_file = new File("/proc/" + pid);
		return pid_file.exists() && (pid_file.lastModified() == date);		
	}	
}
