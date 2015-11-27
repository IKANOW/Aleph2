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
package com.ikanow.aleph2.data_model.interfaces.data_analytics;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

import fj.data.Either;

/** Concrete implementations of this generic context can be requested from the data services using getUnderlyingTechnology
 *  (with the AnalyticJobInputBean passed into the options as JSON using BeanTemplateUtils.toJson(input_job).toString())
 *  eg "interface HadoopInputContext extends IAnalyticsAccessContext<InputFormat>"
 *  and then the analytic technology can use the returned type in a technology specific way to help with the input
 *   NOTE: use the AnalyticsUtils methods to inject implementation from within the data services
 * @author Alex
 */
public interface IAnalyticsAccessContext<T> {

	/** A common access context that the AccessContext will (generally) satisfy
	 * @author Alex
	 */
	public interface GenericCrudAccessContext extends IAnalyticsAccessContext<ICrudService<JsonNode>> {}
	
	/** The class (or an instance of the class, depending on the technology) that can be used to provide the required input/output 
	 * @return Either an instance of the class, or the class itself 
	 */
	Either<T, Class<T>> getAccessService();
	
	/** The configuration that goes along with the access service - this is used in a technology specific way 
	 * @return Optionally, additional configuration
	 */
	Optional<Map<String, Object>> getAccessConfig();
	
	/** Describes the current access context, defaults to some semi-useful description but can be overriden
	 *  at the point of creation
	 * @return a string describing the access context
	 */
	default String describe() {
		//(by default return the keys but not the values in case there's a massive number of them)
		return ErrorUtils.get("service_name={0} options={1}", 
				this.getAccessService().either(l -> l.getClass().getSimpleName(), r -> r.getSimpleName()),
				this.getAccessConfig().map(cfg -> cfg.keySet().stream().collect(Collectors.joining(";"))).orElse("")
				);
	}
}
