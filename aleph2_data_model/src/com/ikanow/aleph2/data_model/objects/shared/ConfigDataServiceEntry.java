package com.ikanow.aleph2.data_model.objects.shared;

import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

public class ConfigDataServiceEntry
{
	public final String annotationName;
	public final Optional<String> interfaceName;
	public final String serviceName;
	public final boolean isDefault;
	
	public ConfigDataServiceEntry(@NonNull String annotationName, Optional<String> interfaceName, @NonNull String serviceName, boolean isDefault) {
		this.annotationName = annotationName;
		this.interfaceName = interfaceName;
		this.serviceName = serviceName;
		this.isDefault = isDefault;
	}
}
