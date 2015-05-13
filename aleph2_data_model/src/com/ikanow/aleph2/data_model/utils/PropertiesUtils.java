package com.ikanow.aleph2.data_model.utils;

import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.objects.shared.ConfigDataServiceEntry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigObject;

public class PropertiesUtils {
	private final static Logger logger = Logger.getLogger(PropertiesUtils.class.getName());		
	
//	public static void applyBindingsFromConfig(Config config, String dataServicesFragement, Binder binder) {
//		//services are in the format
//		//{dataServicesFragement}.{SERVICE_NAME}.interface={fullpath}
//		//{dataServicesFragement}.{SERVICE_NAME}.service={fullpath}
//		Map<Class<?>, Boolean> interfaceHasDefault = new HashMap<Class<?>, Boolean>();
//		ConfigObject dataServiceConfig = config.getObject(dataServicesFragement);
//		dataServiceConfig.entrySet()
//			.forEach(entry -> {
//				String service_name = entry.getKey();
//				String interfaceClazzName = getConfigValue(config, dataServicesFragement+"."+service_name+".interface", null);
//				String serviceClazzName = getConfigValue(config, dataServicesFragement+"."+service_name+".service", null);
//				Boolean defaultService = getConfigValue(config, dataServicesFragement+"."+service_name+".default", false);
//				Class interfaceClazz = null;
//				try
//				{
//					if ( interfaceClazzName != null ) {						
//						interfaceClazz = Class.forName(interfaceClazzName);
//						//check if we've set a default for this interface yet
//						if ( interfaceHasDefault.containsKey(interfaceClazz)) {
//							logger.log(Level.ALL, "Error: " + interfaceClazzName + " already had a default set");
//							//prevent bindings?
//							binder.addError("Error: " + interfaceClazzName + " already had a default set");
//						}
//					}
//					Class serviceClazz = Class.forName(serviceClazzName);
//					bindDataService(serviceClazz, interfaceClazz, service_name, defaultService, binder);
//				} catch (ClassNotFoundException ex) {
//					logger.log(Level.ALL, "Error parsing config class names", ex);
//				}
//			});
//	}
	
	private static String getConfigValue(@NonNull Config config, @NonNull String key, @NonNull String defaultValue) {
		String value;
		try {
			value = config.getString(key);
		} catch (ConfigException ex) {
			value = defaultValue;
		}
		return value;
	}
	
	private static boolean getConfigValue(@NonNull Config config, @NonNull String key, @NonNull boolean defaultValue) {
		boolean value = defaultValue;
		try {
			value = config.getBoolean(key);
		} catch (ConfigException ex) {
			value = defaultValue;
		}
		return value;
	}
	
//	@SuppressWarnings("unchecked")
//	private static void bindDataService(@NonNull Class serviceClazz, Class interfaceClazz, @NonNull String bindingName, boolean isDefault, @NonNull Binder binder) {
//		if ( interfaceClazz != null ) {
//			logger.fine("Binding " + interfaceClazz.getName() + " to " + serviceClazz.getName());			
//			binder.bind(interfaceClazz).annotatedWith(Names.named(bindingName)).to(serviceClazz);
//			if ( isDefault )
//				binder.bind(interfaceClazz).to(serviceClazz);
//		}
//		else {
//			logger.fine("Binding Custom Class " + serviceClazz.getName());
//			//binder.bind(serviceClazz).annotatedWith(Names.named(bindingName));
//			if ( isDefault )
//				binder.bind(serviceClazz);
//		}
//	}

	public static List<ConfigDataServiceEntry> getDataServiceProperties(Config config, String configPrefix) {		
		ConfigObject dataServiceConfig = config.getObject(configPrefix);
		List<ConfigDataServiceEntry> toReturn = dataServiceConfig.entrySet()
			.stream()
			.map(entry -> 
				new ConfigDataServiceEntry(
					entry.getKey(), 
					Optional.ofNullable(PropertiesUtils.getConfigValue(config, configPrefix+"."+entry.getKey()+".interface", null)), 
					PropertiesUtils.getConfigValue(config, configPrefix+"."+entry.getKey()+".service", null),
					Optional.ofNullable(PropertiesUtils.getConfigValue(config, configPrefix+"."+entry.getKey()+".modules", null)),
					PropertiesUtils.getConfigValue(config, configPrefix+"."+entry.getKey()+".default", false))
			)
			.collect( Collectors.toList());
		return toReturn;
	}
}
