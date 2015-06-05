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
package com.ikanow.aleph2.access_manager.data_access;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;


import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_access.IAccessContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ModuleUtils.ServiceContext;

public class AccessContext extends ServiceContext implements IAccessContext {

	@Override
	public CompletableFuture<BasicMessageBean> subscribeToBucket(
			DataBucketBean bucket, Optional<String> stage,
			Consumer<JsonNode> on_new_object_callback) {
		return null;
	}

	@Override
	public CompletableFuture<BasicMessageBean> subscribeToAnalyticThread(
			AnalyticThreadBean analytic_thread,
			Optional<String> stage,
			Consumer<JsonNode> on_new_object_callback) {
		return null;
	}

	@Override
	public CompletableFuture<Stream<JsonNode>> getObjectStreamFromBucket(
			DataBucketBean bucket, Optional<String> stage) {
		return null;
	}

	@Override
	public Stream<JsonNode> getObjectStreamFromAnalyticThread(
			AnalyticThreadBean analytic_thread,
			Optional<String> stage) {
		return null;
	}

	@Override
	public void initializeNewContext(String string) {
	}
	
	

}
