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
 *******************************************************************************/
package com.ikanow.aleph2.management_db.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;

public class TestMgmtCrudUtils {

	//TODO: need a test for applyNodeAffinityWrapper
	
	//NOTE: These functions are quite hard to test standalone, but they are covered by the Test*CrudService tests
	
	// Here's a nice tidy one we can test though:
	
	@Test
	public void test_buildBucketReplyForUser() {
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test")
				.done().get();
		
		final BasicMessageBean standard_retval =
				ErrorUtils.buildSuccessMessage("test", "test", "test");
		
		// standard case: 1 reply, no rejections
		{
			final BucketActionCollectedRepliesMessage test = new 
					BucketActionCollectedRepliesMessage("test", Arrays.asList(standard_retval), Collections.emptySet(), Collections.emptySet());
			
			Collection<BasicMessageBean> replies = MgmtCrudUtils.buildBucketReplyForUser(bucket, CompletableFuture.completedFuture(test)).join();
			
			assertEquals(1, replies.size());
			assertEquals(1L, replies.stream().filter(b -> b.success()).count());
			assertEquals(1L, replies.stream().filter(b -> b.source().equals("test")).count());
		}
		// standard case 2: 1 reply, 5 rejections
		{
			final BucketActionCollectedRepliesMessage test = new 
					BucketActionCollectedRepliesMessage("test", Arrays.asList(standard_retval), Collections.emptySet(), 
							ImmutableSet.of("reject1", "reject2", "reject3", "reject4", "reject5"));
			
			Collection<BasicMessageBean> replies = MgmtCrudUtils.buildBucketReplyForUser(bucket, CompletableFuture.completedFuture(test)).join();
			
			assertEquals(2, replies.size());
			assertEquals(2L, replies.stream().filter(b -> b.success()).count());
			assertEquals(1L, replies.stream().filter(b -> b.source().equals("test")).count());
			final String expected_err_1 = ErrorUtils.get(ManagementDbErrorUtils.SOME_NODES_REJECTED_BUCKET, "/test", 5, "reject1;reject2;reject3");
			
			assertEquals("Should contain rejected info: " + replies.stream().map(b -> b.message()).collect(Collectors.joining(" | ")), 
					1L, replies.stream().filter(b -> b.message().equals(expected_err_1)).count());
		}
		// error case 1: 0 replies, 0 rejections, 0 time outs
		{
			final BucketActionCollectedRepliesMessage test = new 
					BucketActionCollectedRepliesMessage("test", Arrays.asList(), Collections.emptySet(), 
							ImmutableSet.of());
			
			Collection<BasicMessageBean> replies = MgmtCrudUtils.buildBucketReplyForUser(bucket, CompletableFuture.completedFuture(test)).join();
			
			assertEquals(1, replies.size());
			assertEquals(0L, replies.stream().filter(b -> b.success()).count());
			assertEquals(0L, replies.stream().filter(b -> b.source().equals("test")).count());
			final String expected_err_1 = ErrorUtils.get(ManagementDbErrorUtils.NO_NODES_AVAILABLE);
			
			assertEquals("Should contain no node error: " + replies.stream().map(b -> b.message()).collect(Collectors.joining(" | ")), 
					1L, replies.stream().filter(b -> b.message().equals(expected_err_1)).count());
		}
		// error case 2: 0 replies, 2 rejections, 0 timeouts
		{
			final BucketActionCollectedRepliesMessage test = new 
					BucketActionCollectedRepliesMessage("test", Arrays.asList(), Collections.emptySet(), 
							ImmutableSet.of("reject1", "reject2"));
			
			Collection<BasicMessageBean> replies = MgmtCrudUtils.buildBucketReplyForUser(bucket, CompletableFuture.completedFuture(test)).join();
			
			assertEquals(2, replies.size());
			assertEquals(1L, replies.stream().filter(b -> b.success()).count());
			assertEquals(0L, replies.stream().filter(b -> b.source().equals("test")).count());
			final String expected_err_1 = ErrorUtils.get(ManagementDbErrorUtils.ALL_NODES_REJECTED_BUCKET, "/test");
			final String expected_err_2 = ErrorUtils.get(ManagementDbErrorUtils.SOME_NODES_REJECTED_BUCKET, "/test", 2, "reject1;reject2");
			
			assertEquals("Should contain rejected error: " + replies.stream().map(b -> b.message()).collect(Collectors.joining(" | "))
					+ " vs " + expected_err_1
					, 
					1L, replies.stream().filter(b -> b.message().equals(expected_err_1)).count()
					);
			assertEquals("Should contain rejected info: " + replies.stream().map(b -> b.message()).collect(Collectors.joining(" | "))
					+ " vs " + expected_err_2
					, 
					1L, replies.stream().filter(b -> b.message().equals(expected_err_2)).count()
					);
		}
		// error case 3: 0 replies, 1 rejections, 2 timeouts
		{
			final BucketActionCollectedRepliesMessage test = new 
					BucketActionCollectedRepliesMessage("test", Arrays.asList(), 
							ImmutableSet.of("timed_out1", "timed_out2"), 
							ImmutableSet.of("reject1"));
			
			Collection<BasicMessageBean> replies = MgmtCrudUtils.buildBucketReplyForUser(bucket, CompletableFuture.completedFuture(test)).join();
			
			assertEquals(3, replies.size());
			assertEquals(2L, replies.stream().filter(b -> b.success()).count());
			assertEquals(0L, replies.stream().filter(b -> b.source().equals("test")).count());
			final String expected_err_1 = ErrorUtils.get(ManagementDbErrorUtils.ALL_NODES_TIMED_OUT_OR_REJECTED, "/test", 2, 1);
			final String expected_err_2 = ErrorUtils.get(ManagementDbErrorUtils.SOME_NODES_TIMED_OUT, "/test", 2, "timed_out1;timed_out2");
			final String expected_err_3 = ErrorUtils.get(ManagementDbErrorUtils.SOME_NODES_REJECTED_BUCKET, "/test", 1, "reject1");
			
			assertEquals("Should contain timedout error: " + replies.stream().map(b -> b.message()).collect(Collectors.joining(" | "))
					+ " vs " + expected_err_1
					, 
					1L, replies.stream().filter(b -> b.message().equals(expected_err_1)).count()
					);
			assertEquals("Should contain timedout info: " + replies.stream().map(b -> b.message()).collect(Collectors.joining(" | "))
					+ " vs " + expected_err_2
					, 
					1L, replies.stream().filter(b -> b.message().equals(expected_err_2)).count()
					);
			assertEquals("Should contain rejected info: " + replies.stream().map(b -> b.message()).collect(Collectors.joining(" | "))
					+ " vs " + expected_err_3
					, 
					1L, replies.stream().filter(b -> b.message().equals(expected_err_3)).count()
					);
		}
		// error case 4: 0 replies, 0 rejections, 5 timeouts
		{
			final BucketActionCollectedRepliesMessage test = new 
					BucketActionCollectedRepliesMessage("test", Arrays.asList(), 
							ImmutableSet.of("timed_out1", "timed_out2", "timed_out3", "timed_out4", "timed_out5"), 
							ImmutableSet.of());
			
			Collection<BasicMessageBean> replies = MgmtCrudUtils.buildBucketReplyForUser(bucket, CompletableFuture.completedFuture(test)).join();
			
			assertEquals(2, replies.size());
			assertEquals(1L, replies.stream().filter(b -> b.success()).count());
			assertEquals(0L, replies.stream().filter(b -> b.source().equals("test")).count());
			final String expected_err_1 = ErrorUtils.get(ManagementDbErrorUtils.ALL_NODES_TIMED_OUT, "/test");
			final String expected_err_2 = ErrorUtils.get(ManagementDbErrorUtils.SOME_NODES_TIMED_OUT, "/test", 5, "timed_out1;timed_out2;timed_out3");
			
			assertEquals("Should contain timedout error: " + replies.stream().map(b -> b.message()).collect(Collectors.joining(" | "))
					+ " vs " + expected_err_1
					, 
					1L, replies.stream().filter(b -> b.message().equals(expected_err_1)).count()
					);
			assertEquals("Should contain timedout info: " + replies.stream().map(b -> b.message()).collect(Collectors.joining(" | "))
					+ " vs " + expected_err_2
					, 
					1L, replies.stream().filter(b -> b.message().equals(expected_err_2)).count()
					);
		}
	}
}
