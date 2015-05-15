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
package com.ikanow.aleph2.data_model.interfaces.data_services.samples;

import java.util.Collection;
import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

public class SampleDocumentService implements IDocumentService {

	@Override
	public <O> @NonNull ICrudService<O> getCrudService(@NonNull Class<O> clazz,
			@NonNull DataBucketBean bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <O> @NonNull ICrudService<O> getCrudService(@NonNull Class<O> clazz,
			@NonNull Collection<DataBucketBean> buckets) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> @NonNull T getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}

}
