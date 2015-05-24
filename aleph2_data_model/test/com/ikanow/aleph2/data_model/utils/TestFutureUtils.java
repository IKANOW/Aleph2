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
package com.ikanow.aleph2.data_model.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

public class TestFutureUtils {

	public static class TestBean {
		String field1;
	}
	
	@Test
	public void testWrappedManagementFuture() throws InterruptedException, ExecutionException, TimeoutException {
		
		// Test1 - very basic stuff
		
		final CompletableFuture<TestBean> pretest1 = 
				CompletableFuture.completedFuture(BeanTemplateUtils.build(TestBean.class).with("field1", "alskdjfhg").done().get());
		
		final ManagementFuture<TestBean> test1 = FutureUtils.createManagementFuture(pretest1);
		
		assertEquals(Collections.emptyList(), test1.getManagementResults().get());
		
		assertEquals(pretest1.get().field1, test1.get().field1);
		
		assertEquals(pretest1.isDone(), test1.isDone());
		assertEquals(pretest1.isCancelled(), test1.isCancelled());		
		assertEquals(false, test1.cancel(true));
		assertEquals(pretest1.isCancelled(), test1.isCancelled());		
		
		assertEquals(pretest1.get().field1, test1.get(1000, TimeUnit.SECONDS).field1);		
		
		final CompletableFuture<TestBean> test1b = test1.thenApply(e -> e);
		assertEquals(pretest1.get().field1, test1b.get().field1);

		final CompletableFuture<Collection<BasicMessageBean>> test1c = test1.getManagementResults().thenApply(e -> e);
		assertEquals(Collections.emptyList(), test1c.get());
		
		// Test2 - same but with a side channel 
		
		final CompletableFuture<TestBean> pretest2a = 
				CompletableFuture.completedFuture(BeanTemplateUtils.build(TestBean.class).with("field1", "zmxncvb").done().get());
		
		final BasicMessageBean bean_pretest2b = BeanTemplateUtils.build(BasicMessageBean.class).with(BasicMessageBean::command, "qpwoeirutry").done().get(); 
		
		final CompletableFuture<Collection<BasicMessageBean>> pretest2b = CompletableFuture.completedFuture(Arrays.asList(bean_pretest2b));

		ManagementFuture<TestBean> test2 = FutureUtils.createManagementFuture(pretest2a, pretest2b);
		
		assertEquals(pretest2a.get().field1, test2.get().field1);		
		assertEquals(pretest2a.isDone(), test2.isDone());
		assertEquals(pretest2a.isCancelled(), test2.isCancelled());		
		assertEquals(false, test2.cancel(true));
		assertEquals(pretest2a.isCancelled(), test2.isCancelled());				
		assertEquals(pretest2a.get().field1, test2.get(1000, TimeUnit.SECONDS).field1);

		assertEquals(pretest2b.get(), test2.getManagementResults().get());		
		assertEquals(pretest2b.isDone(), test2.getManagementResults().isDone());
		assertEquals(pretest2b.isCancelled(), test2.getManagementResults().isCancelled());		
		assertEquals(false, test2.getManagementResults().cancel(true));
		assertEquals(pretest2b.isCancelled(), test2.getManagementResults().isCancelled());				
		assertEquals(pretest2b.get(), test2.getManagementResults().get(1000, TimeUnit.SECONDS));
		
		final CompletableFuture<Collection<BasicMessageBean>> test2c = test2.getManagementResults().thenApply(e -> e);
		assertEquals(pretest2b.get(), test2c.get());		
		
		// Test 3 - nested

		final ExecutorService executor = Executors.newFixedThreadPool(5);
		final CompletableFuture<TestBean> pretest3a = CompletableFuture.supplyAsync(() -> {
			try { TimeUnit.SECONDS.sleep(1); } catch (Exception e) {}
			return pretest1.join();
		}, executor);
		
		final CompletableFuture<Collection<BasicMessageBean>> pretest3b = CompletableFuture.supplyAsync(() -> {
			try { TimeUnit.SECONDS.sleep(2); } catch (Exception e) {}
			return Collections.emptyList();
		}, executor);
		
		final CompletableFuture<ManagementFuture<TestBean>> test3 = CompletableFuture.supplyAsync(() -> {
			try { TimeUnit.SECONDS.sleep(1); } catch (Exception e) {}
			return FutureUtils.createManagementFuture(pretest3a, pretest3b);
		}, executor);		
		
		final ManagementFuture<TestBean> test3_actual = FutureUtils.denestManagementFuture(test3);
		
		assertEquals(pretest1.get(), test3_actual.get());		
		assertEquals(Collections.emptyList(), test3_actual.getManagementResults().get());		

		final CompletableFuture<TestBean> test3_actual_1 = test3_actual.thenApply(e -> e);
		final CompletableFuture<Collection<BasicMessageBean>> test3_actual_2 = test3_actual.getManagementResults().thenApply(e -> e);
		
		assertEquals(pretest1.get(), test3_actual_1.get());		
		assertEquals(Collections.emptyList(), test3_actual_2.get());		
	}

	@Test
	public void testWrappedErrorFuture() throws InterruptedException, ExecutionException {
		final RuntimeException e = new RuntimeException("test1");
		final CompletableFuture<Object> test1 = FutureUtils.returnError(e);
		try {
			test1.get();
			fail("Should have thrown exception"); 
		}
		catch (Throwable t) {
			assertEquals(e, t.getCause());
		}
		
		final CompletableFuture<Object> test2 = test1.thenApply(oo -> oo);
		try {
			test2.get();
			fail("Should have thrown exception"); 
		}
		catch (Throwable t) {
			assertEquals(e, t.getCause());
		}
	}
	
	@Test
	public void testWrappedScalaFuture() {
		//(tested under management_db_service, we don't have akka here so it's a bit hard to test)		
	}
	
	/** This function doesn't test any Aleph2 code, just want to verify how exceptions propagate
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test 
	public void composingAndApplyFuturesAndExceptions() throws InterruptedException, ExecutionException {

		CompletableFuture<String> test1a = CompletableFuture.completedFuture("test");
		
		// Composing
		
		CompletableFuture<String> test1b = test1a
				.thenCompose(s -> CompletableFuture.completedFuture(s + "1"))
				.thenCompose(ss -> CompletableFuture.completedFuture(ss + "1"));
		
		assertEquals("test11", test1b.get());
		
		CompletableFuture<String> test2b = test1a
				.thenCompose(s -> CompletableFuture.completedFuture(s + "1"))
				.thenCompose(ss -> {
					if (ss.equals("test1")) {
						throw new RuntimeException("test2b");
					}
					return CompletableFuture.completedFuture(ss + "1");
				})
				.thenCompose(sss -> {
					System.out.println("NEVER SEE THIS1");
					return CompletableFuture.completedFuture(sss + "1");
				});
		
		try {
			test2b.get();
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals("test2b", e.getCause().getMessage());
		}

		// Same again when applying
		
		CompletableFuture<String> test3b = test1a
				.thenApply(s -> s + 1)
				.thenApply(ss -> ss + 1)
				;
		
		assertEquals("test11", test3b.get());

		CompletableFuture<String> test4b = test1a
				.thenApply(s -> s + 1)
				.thenApply(ss -> {
					if (ss.equals("test1")) {
						throw new RuntimeException("test4b");
					}
					return ss + "1";
				})
				.thenApply(sss -> {
					System.out.println("NEVER SEE THIS2");
					return sss + 1;
				})
				;
		
		try {
			test4b.get();
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals("test4b", e.getCause().getMessage());
		}		
	}
}
