package com.ikanow.aleph2.access_manager.data_access;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_access.IAccessContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class AccessContext implements IAccessContext {

	private final static Logger logger = Logger.getLogger(AccessContext.class.getName());	

	@Override
	public CompletableFuture<BasicMessageBean> subscribeToBucket(
			@NonNull DataBucketBean bucket, @NonNull Optional<String> stage,
			Consumer<JsonNode> on_new_object_callback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<BasicMessageBean> subscribeToAnalyticThread(
			@NonNull AnalyticThreadBean analytic_thread,
			@NonNull Optional<String> stage,
			Consumer<JsonNode> on_new_object_callback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Stream<JsonNode>> getObjectStreamFromBucket(
			@NonNull DataBucketBean bucket, @NonNull Optional<String> stage) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<JsonNode> getObjectStreamFromAnalyticThread(
			@NonNull AnalyticThreadBean analytic_thread,
			@NonNull Optional<String> stage) {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
