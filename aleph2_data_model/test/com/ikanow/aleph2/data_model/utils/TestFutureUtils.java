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
	public void testFutureUtils() throws InterruptedException, ExecutionException, TimeoutException {
		
		// Test1 - very basic stuff
		
		final CompletableFuture<TestBean> pretest1 = 
				CompletableFuture.completedFuture(BeanTemplateUtils.build(TestBean.class).with("field1", "alskdjfhg").done().get());
		
		ManagementFuture<TestBean> test1 = FutureUtils.createManagementFuture(pretest1);
		
		assertEquals(Collections.emptyList(), test1.getManagementResults().get());
		
		assertEquals(pretest1.get().field1, test1.get().field1);
		
		assertEquals(pretest1.isDone(), test1.isDone());
		assertEquals(pretest1.isCancelled(), test1.isCancelled());		
		assertEquals(false, test1.cancel(true));
		assertEquals(pretest1.isCancelled(), test1.isCancelled());		
		
		assertEquals(pretest1.get().field1, test1.get(1000, TimeUnit.SECONDS).field1);		
		
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
	}
	
}
