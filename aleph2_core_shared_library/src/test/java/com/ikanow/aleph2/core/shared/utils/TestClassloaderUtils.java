package com.ikanow.aleph2.core.shared.utils;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;

import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

import fj.data.Validation;

public class TestClassloaderUtils {

	public static class TestMessageBean {}
	
	@Test
	public void testClassLoading_primaryLib() throws UnsupportedFileSystemException {

		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
		
		final String pathname = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example.jar";
		final Path path = new Path(pathname);
		final Path path2 = FileContext.getLocalFSFileContext().makeQualified(path);
		
		final Validation<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleHarvestTechnology",
						Optional.of(path2.toString()),
						Collections.emptyList(), "test1", new TestMessageBean());						
						
		if (ret_val.isFail()) {
			System.out.println("About to crash with: " + ret_val.fail().message());
		}		
		assertEquals(true, ret_val.success().canRunOnThisNode(BeanTemplateUtils.build(DataBucketBean.class).done().get(), null));
		
		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("STILL! Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
	}
	
	@Test
	public void testClassLoading_secondaryLib() throws UnsupportedFileSystemException {

		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
		
		final String pathname = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example.jar";
		final Path path = new Path(pathname);
		final Path path2 = FileContext.getLocalFSFileContext().makeQualified(path);		
		
		final Validation<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleHarvestTechnology", 
						Optional.empty(),
						Arrays.asList(path2.toString()), 
						"test1", new TestMessageBean());						
						
		if (ret_val.isFail()) {
			System.out.println("About to crash with: " + ret_val.fail().message());
		}		
		assertEquals(true, ret_val.success().canRunOnThisNode(BeanTemplateUtils.build(DataBucketBean.class).done().get(), null));
		
		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("STILL! Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
	}
	
	@Test
	public void testClassLoading_fails() throws UnsupportedFileSystemException {

		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
		
		final String pathname = System.getProperty("user.dir") + "/simple-harvest-examplee-FAILS.jar";
		final Path path = new Path(pathname);
		final Path path2 = FileContext.getLocalFSFileContext().makeQualified(path);				
		
		final Validation<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleHarvestTechnology", 
						Optional.empty(),
						Arrays.asList(path2.toString()), 
						"test1", new TestMessageBean());						
						
		if (ret_val.isSuccess()) {
			System.out.println("About to crash,found class?");
		}		
		BasicMessageBean error = ret_val.fail();
		
		assertEquals(error.command(), "TestMessageBean");
		assertEquals((double)error.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(error.details(), null);
		final String expected_err_fragment = "Error loading class com.ikanow.aleph2.test.example.ExampleHarvestTechnology: [java.lang.ClassNotFoundException: com.ikanow.aleph2.test.example.ExampleHarvestTechnology: JclException]";
		assertTrue("Failed error message, should contain: " + expected_err_fragment + " vs " + error.message(), 
				error.message().contains(expected_err_fragment));
		assertEquals(error.message_code(), null);
		assertEquals(error.source(), "test1");
		assertEquals(error.success(), false);
		
	}
	
	@Test
	public void testClassLoading_wrongInterface() throws UnsupportedFileSystemException {

		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
		
		final String pathname = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example.jar";
		final Path path = new Path(pathname);
		final Path path2 = FileContext.getLocalFSFileContext().makeQualified(path);				
		
		final Validation<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"java.lang.String", 
						Optional.empty(),
						Arrays.asList(path2.toString()), 
						"test1", new TestMessageBean());						
						
		if (ret_val.isSuccess()) {
			System.out.println("About to crash,found class?");
		}		
		BasicMessageBean error = ret_val.fail();
		
		assertEquals(error.command(), "TestMessageBean");
		assertEquals((double)error.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(error.details(), null);
		final String expected_err_fragment = "Error: class java.lang.String is not an implementation of interface";
		assertTrue("Failed error message, should contain: " + expected_err_fragment + " vs " + error.message(), 
				error.message().contains(expected_err_fragment));
		assertEquals(error.message_code(), null);
		assertEquals(error.source(), "test1");
		assertEquals(error.success(), false);
		
	}
	
}
