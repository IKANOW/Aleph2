package com.ikanow.aleph2.data_import_manager.harvest.utils;

import static org.junit.Assert.*;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;

import com.ikanow.aleph2.data_import_manager.utils.ClassloaderUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

import fj.data.Either;

public class TestClassloaderUtils {

	public static class TestMessageBean {}
	
	@Test
	public void testClassLoading_primaryLib() {

		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleHarvestTechnology", 
						Optional.of("file:/Users/acp/github/Aleph2/aleph2_data_import_manager/simple-harvest-example.jar"),
						Collections.emptyList(), "test1", new TestMessageBean());						
						
		if (ret_val.isLeft()) {
			System.out.println("About to crash with: " + ret_val.left().value().message());
		}		
		assertEquals(true, ret_val.right().value().canRunOnThisNode(BeanTemplateUtils.build(DataBucketBean.class).done().get()));
		
		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("STILL! Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
	}
	
	@Test
	public void testClassLoading_secondaryLib() {

		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleHarvestTechnology", 
						Optional.empty(),
						Arrays.asList("file:/Users/acp/github/Aleph2/aleph2_data_import_manager/simple-harvest-example.jar"), 
						"test1", new TestMessageBean());						
						
		if (ret_val.isLeft()) {
			System.out.println("About to crash with: " + ret_val.left().value().message());
		}		
		assertEquals(true, ret_val.right().value().canRunOnThisNode(BeanTemplateUtils.build(DataBucketBean.class).done().get()));
		
		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("STILL! Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
	}
	
	@Test
	public void testClassLoading_fails() {

		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			assertTrue("Should have thrown a ClassNotFoundException", false);
		}
		catch (ClassNotFoundException e) {
			//expected!
		}
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleHarvestTechnology", 
						Optional.empty(),
						Arrays.asList("file:/Users/acp/github/Aleph2/aleph2_data_import_manager/simple-harvest-example-FAILS.jar"), 
						"test1", new TestMessageBean());						
						
		if (ret_val.isRight()) {
			System.out.println("About to crash,found class?");
		}		
		BasicMessageBean error = ret_val.left().value();
		
		assertEquals(error.command(), "TestMessageBean");
		assertEquals((double)error.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(error.details(), null);
		assertEquals(error.message(), "Error loading class com.ikanow.aleph2.test.example.ExampleHarvestTechnology: [java.lang.ClassNotFoundException: com.ikanow.aleph2.test.example.ExampleHarvestTechnology: JclException]:[JclObjectFactory.java:102:org.xeustechnologies.jcl.JclObjectFactory:create][JclObjectFactory.java:85:org.xeustechnologies.jcl.JclObjectFactory:create][ClassloaderUtils.java:66:com.ikanow.aleph2.data_import_manager.utils.ClassloaderUtils:getFromCustomClasspath][TestClassloaderUtils.java:99:com.ikanow.aleph2.data_import_manager.harvest.utils.TestClassloaderUtils:testClassLoading_fails] ([com.ikanow.aleph2.test.example.ExampleHarvestTechnology: ClassNotFoundException]:[AbstractClassLoader.java:129:org.xeustechnologies.jcl.AbstractClassLoader:loadClass][JclObjectFactory.java:85:org.xeustechnologies.jcl.JclObjectFactory:create][ClassloaderUtils.java:66:com.ikanow.aleph2.data_import_manager.utils.ClassloaderUtils:getFromCustomClasspath][TestClassloaderUtils.java:99:com.ikanow.aleph2.data_import_manager.harvest.utils.TestClassloaderUtils:testClassLoading_fails])");
		assertEquals(error.message_code(), null);
		assertEquals(error.source(), "test1");
		assertEquals(error.success(), false);
		
	}
}
