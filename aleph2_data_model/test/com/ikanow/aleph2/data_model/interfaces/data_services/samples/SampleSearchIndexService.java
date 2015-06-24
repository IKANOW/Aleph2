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
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class SampleSearchIndexService implements ISearchIndexService {

	@Override
	public <O> ICrudService<O> getCrudService(Class<O> clazz,
			DataBucketBean bucket) {
		return null;
	}

	@Override
	public <O> ICrudService<O> getCrudService(Class<O> clazz,
			Collection<String> buckets) {
		return null;
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(
			Class<T> driver_class, Optional<String> driver_options) {
		return null;
	}

	@Override
	public List<BasicMessageBean> validateSchema(SearchIndexSchemaBean schema, final DataBucketBean bucket) {
		return Collections.emptyList();
	}

	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return null;
	}
}
