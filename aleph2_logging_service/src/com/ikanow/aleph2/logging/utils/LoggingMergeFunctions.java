/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.logging.utils;

import java.util.Optional;
import java.util.function.BiFunction;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/**
 * Collection of merge functions and log frequency rules.
 * @author Burch
 *
 */
public class LoggingMergeFunctions {
	public static final String COUNT_FIELD = "merge_count";
	public static final String SUM_FIELD = "merge_sum";
	public static final String MIN_FIELD = "merge_min";
	public static final String MAX_FIELD = "merge_max";	

	/**
	 * Just returns the newer BasicMessageBean
	 * @return
	 */
	public static BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean> replaceMessage() {
		return (n,o)->{
			return n;
		};
	}
	
	/**
	 * Appends the old BasicMessageBean.message() to the end of the new BasicMessageBean.message(), 
	 * inherits all the fields of the new BasicMessageBean (e.g. does not merge any other fields).
	 */
	public static BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean> appendMessage() {
		return (n,o)->{
			return BeanTemplateUtils.clone(n).with(BasicMessageBean::message, n.message().concat(Optional.ofNullable(o).map(old->" " + old.message()).orElse(""))).done();
		};
	}
	
	/**
	 * Keeps a running count of every message in the BasicMessageBean.details.merge_count field (e.g. adds 1 to that field per call)
	 */
	public static BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean> countMessages() {	
		return (n,o)->{
			Long count = 
					Optional.ofNullable(LoggingUtils.getDetailsMapValue(o, COUNT_FIELD, Long.class)).orElse(0L)
					+ 1L; //add 1 to the count					
			return BeanTemplateUtils.clone(n).with(BasicMessageBean::details, LoggingUtils.mergeDetailsAddValue(o, n, COUNT_FIELD, count)).done();
		};
	}
	
	/**
	 * Keeps a running sum of the value in BasicMessageBean.details.merge_sum field (assumes field is a double), 
	 * inherits all the fields of the new BasicMessageBean (e.g. does not merge any other fields). 
	 * Merges all details of old bmb into new bmb (doesn't overwrite), this is the only way to pass forward values from chaining merge functions. 
	 * (e.g. merge_fn_1 sets field out1:5, merge_fn_2 sets field out2:12, I need to move forward out1 or it will be lost)
	 */	
	public static BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean> sumField(final String field_to_sum) {		
		return sumField(field_to_sum, SUM_FIELD, false);		
	}
	public static BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean> sumField(final String field_to_sum, final String field_for_ouput, final boolean substituteMessage) {		
		return (n,o)->{
			Double sum = 
				Optional.ofNullable(LoggingUtils.getDetailsMapValue(o, field_for_ouput, Double.class)).orElse(0D) //old sum, is stored in output field
				+ Optional.ofNullable(LoggingUtils.getDetailsMapValue(n, field_to_sum, Double.class)).orElse(0D); //add new sum	is stored in input field									
			return BeanTemplateUtils.clone(n).with(BasicMessageBean::details, LoggingUtils.mergeDetailsAddValue(o, n, field_for_ouput, sum)).done();
		};
	}
		
	/**
	 * Copies the smaller field_to_min into the new BMB.  Expects field_to_min to be a double in bmb.details()
	 * Copies min into LoggingFunctions.MIN_FIELD
	 * @param field_to_min
	 * @return
	 */
	public static BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean> minField(final String field_to_min) { 
		return (n,o)->{	
			Double valOld = Optional.ofNullable(LoggingUtils.getDetailsMapValue(o, field_to_min, Double.class)).orElse(null);
			Double valNew = Optional.ofNullable(LoggingUtils.getDetailsMapValue(n, field_to_min, Double.class)).orElse(null);
			//set min field to min or null if neither details had a value				
			return BeanTemplateUtils.clone(n).with(BasicMessageBean::details, LoggingUtils.mergeDetailsAddValue(o, n, MIN_FIELD, Optional.ofNullable(getMinMaxOrNull(valOld, valNew, true)).orElse(null))).done();
		};
	}
	
	/**
	 * Copies the larger field_to_max into the new BMB.  Expects field_to_max to be a double in bmb.details()
	 * Copies max into LoggingFunctions.MAX_FIELD
	 * @param field_to_max
	 * @return
	 */
	public static BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean> maxField(final String field_to_max) { 
		return (n,o)->{
			Double valOld = Optional.ofNullable(LoggingUtils.getDetailsMapValue(o, field_to_max, Double.class)).orElse(null);
			Double valNew = Optional.ofNullable(LoggingUtils.getDetailsMapValue(n, field_to_max, Double.class)).orElse(null);
			//set min field to min or null if neither details had a value				
			return BeanTemplateUtils.clone(n).with(BasicMessageBean::details, LoggingUtils.mergeDetailsAddValue(o, n, MAX_FIELD, Optional.ofNullable(getMinMaxOrNull(valOld, valNew, false)).orElse(null))).done();
		};
	}
	
	/**
	 * Copies both the smaller and larger field into the new BMB.  Expects field_to_min_max to be a double in bmb.details().
	 * Copies min and max into LoggingFunctions.MIN_FIELD and LoggingFunctions.MAX_FIELD
	 * @param field_to_min_max
	 * @return
	 */
	public static BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean> minMaxField(final String field_to_min_max) {
		return (n,o)->{
			Double valOld = Optional.ofNullable(LoggingUtils.getDetailsMapValue(o, field_to_min_max, Double.class)).orElse(null);
			Double valNew = Optional.ofNullable(LoggingUtils.getDetailsMapValue(n, field_to_min_max, Double.class)).orElse(null);
			//set min field to min or null if neither details had a value				
			return BeanTemplateUtils.clone(n).with(BasicMessageBean::details,					
					LoggingUtils.mergeDetailsAddValue(o, n,
							MIN_FIELD, Optional.ofNullable(getMinMaxOrNull(valOld, valNew, true)).orElse(null),
							MAX_FIELD, Optional.ofNullable(getMinMaxOrNull(valOld, valNew, false)).orElse(null))
					).done();
		};
	}	
	
	/**
	 * Returns the smaller of val1 and val2, they can be null and won't be used in determining the smaller if so.
	 * If both values are null, returns null.
	 *
	 * @param val1
	 * @param val2
	 * @return
	 */
	public static Double getMinMaxOrNull(final Double val1, final Double val2, final boolean returnMin) {
		if ( val1 != null ) {
			if ( val2 != null ) {
				if ( val1 < val2 )
					return returnMin ? val1 : val2;					
				else
					return returnMin ? val2 : val1;
			} else 
				return val1;			
		} else if ( val2 != null )
			return val2;
		else
			return null;
	}
}
