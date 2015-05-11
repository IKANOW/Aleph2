package com.ikanow.aleph2.data_model.utils;

import com.google.inject.Injector;

public class GuiceUtils {
	private static Injector injector = null;

	public static Injector getInjector() {
		return injector;
	}
}
