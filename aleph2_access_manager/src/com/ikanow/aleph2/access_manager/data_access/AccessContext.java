package com.ikanow.aleph2.access_manager.data_access;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.ikanow.aleph2.data_model.interfaces.data_access.IAccessContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGeospatialService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AccessContext implements IAccessContext {

	private final static Logger logger = Logger.getLogger(AccessContext.class.getName());
	private static final String DATA_SERVICE_CONFIG_NAME = "data_service";
	private static Map<Key, Object> dataServices; //TODO set this up via config reader
	private static Injector injector;
	private static Map<Class<?>, String> defaultServiceNames = new HashMap<Class<?>, String>();
	
	public AccessContext(Injector injector) {
		AccessContext.injector = injector;
		dataServices = new HashMap<Key, Object>();
	}
	
	//TODO need some way to load this up when the app starts?
//	public static void initialize() {
//		//setup default serviceNameMapping
//		defaultServiceNames.put(ISecurityService.class, "SecurityService");
//		
//		injector = Guice.createInjector(new AccessContext());
//		//IAccessContext ac = injector.getInstance(IAccessContext.class);
//		ContextUtils.setAccessContext(new AccessContext());
//	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <I> I getDataService(@NonNull Class<I> serviceClazz, @NonNull Optional<String> serviceName) {
		Key key;
		if ( serviceName.isPresent() )
			key = Key.get(serviceClazz, Names.named(serviceName.get()));
		else
			key = Key.get(serviceClazz);
			//serviceName = Optional.of(getDefaultServiceName(serviceClazz));
		//Key key = Key.get(serviceClazz, Names.named(serviceName.get()));
		//Key key = Key.get(serviceClazz);
		if ( dataServices == null ) {
			try {
//				loadDataServices();
			} catch (Exception ex) {
				logger.log(Level.ALL, "Error while loading access manager config", ex);				
			}
		}
		if ( !dataServices.containsKey(key) )
			dataServices.put(key, injector.getInstance(key));
		return (I) dataServices.get(key);
	}
	
	private String getDefaultServiceName(Class<?> serviceClazz) {
		if ( defaultServiceNames.containsKey(serviceClazz) )
			return defaultServiceNames.get(serviceClazz);
		return "default";
	}

//	private void loadDataServices() throws ClassNotFoundException {
//		dataServices = new HashMap<Key, Object>();
//		Config config = ConfigFactory.load();
//		PropertiesUtils.applyBindingsFromConfig(config, DATA_SERVICE_CONFIG_NAME, this.binder());
//	}

//	@Override
//	protected void configure() {
//		try {			
//			loadDataServices();
//		} catch (ClassNotFoundException e) {
//			logger.log(Level.ALL, "Error parsing config", e);
//		}
//	}
	
	@Override
	public IColumnarService getColumnarService() {
		return getDataService(IColumnarService.class, getEmptyStringOptional());
	}

	@Override
	public IDocumentService getDocumentService() {
		return getDataService(IDocumentService.class, getEmptyStringOptional());
	}

	@Override
	public IGeospatialService getGeospatialService() {
		return getDataService(IGeospatialService.class, getEmptyStringOptional());
	}

	@Override
	public IGraphService getGraphService() {
		return getDataService(IGraphService.class, getEmptyStringOptional());
	}

	@Override
	public IManagementDbService getManagementDbService() {
		return getDataService(IManagementDbService.class, getEmptyStringOptional());
	}

	@Override
	public ISearchIndexService getSearchIndexService() {
		return getDataService(ISearchIndexService.class, getEmptyStringOptional());
	}

	@Override
	public IStorageService getStorageIndexService() {
		return getDataService(IStorageService.class, getEmptyStringOptional());
	}

	@Override
	public ITemporalService getTemporalService() {
		return getDataService(ITemporalService.class, getEmptyStringOptional());
	}

	@Override
	public ISecurityService getSecurityService() {
		return getDataService(ISecurityService.class, getEmptyStringOptional());
	}

	@Override
	public Future<BasicMessageBean> subscribeToBucket(
			@NonNull DataBucketBean bucket, @NonNull Optional<String> stage,
			Consumer<JsonNode> on_new_object_callback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Future<BasicMessageBean> subscribeToAnalyticThread(
			@NonNull AnalyticThreadBean analytic_thread,
			@NonNull Optional<String> stage,
			Consumer<JsonNode> on_new_object_callback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Future<Stream<JsonNode>> getObjectStreamFromBucket(
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
	
	public static Optional<String> getEmptyStringOptional() {
		Optional<String> emptyOptional = Optional.empty();
		return emptyOptional;
	}

}
