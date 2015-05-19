/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.utils;

import java.util.concurrent.CompletableFuture;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

import fj.data.Either;

/** Utilities for retrieving shared JARs to a local spot from where they can easily be used by a classloader
 * @author acp
 */
public class JarCacheUtils {

	/** Moves a shared JAR into a local spot (if required)
	 * @param library_bean
	 * @param fs
	 * @return
	 */
	@NonNull
	public static CompletableFuture<Either<BasicMessageBean, String>> getCachedJar(
			final @NonNull SharedLibraryBean library_bean, final @NonNull IStorageService fs)
	{
		//TODO
		return null;
	}
}
