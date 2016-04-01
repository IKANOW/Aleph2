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
package com.ikanow.aleph2.data_model.objects.shared;

import java.util.Date;
import java.util.Map;
import java.util.function.Supplier;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier;
import com.ikanow.aleph2.data_model.utils.SetOnce;

/**
 * @author Burch
 *
 */
public class BasicMessageBeanSupplier implements IBasicMessageBeanSupplier {
	final SetOnce<Boolean> success = new SetOnce<Boolean>();
	final SetOnce<Supplier<String>> source = new SetOnce<Supplier<String>>();
	final SetOnce<Supplier<String>> command = new SetOnce<Supplier<String>>();
	final SetOnce<Supplier<Integer>> message_code = new SetOnce<Supplier<Integer>>();
	final SetOnce<Supplier<String>> message = new SetOnce<Supplier<String>>();
	final SetOnce<Supplier<Map<String, Object>>> details = new SetOnce<Supplier<Map<String,Object>>>();
	final SetOnce<BasicMessageBean> bmb = new SetOnce<BasicMessageBean>();
	/**
	 * @param success
	 * @param source
	 * @param command
	 * @param message
	 * @param details
	 */
	public BasicMessageBeanSupplier(boolean success, Supplier<String> source,
			Supplier<String> command,
			Supplier<Integer> message_code,
			Supplier<String> message,
			Supplier<Map<String, Object>> details) {
		this.success.set(success);
		this.source.set(source);
		this.command.set(command);
		this.message_code.set(message_code);
		this.message.set(message);
		this.details.set(details);
	}

	/**
	 * Builds a supplier from a BMB for utility
	 * @param message2
	 */
	public BasicMessageBeanSupplier(final BasicMessageBean bmb) {		
		this.bmb.set(bmb);
		this.source.set(()->bmb.source());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier#getBasicMessageBean()
	 */
	@Override
	public BasicMessageBean getBasicMessageBean() {
		if ( !this.bmb.isSet() )
			this.bmb.set(new BasicMessageBean(new Date(), this.success.get(), this.source.get().get(), this.command.get().get(), this.message_code.get().get(), this.message.get().get(), this.details.get().get()));
		return this.bmb.get();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier#getSubsystem()
	 */
	@Override
	public String getSubsystem() {
		return this.source.get().get();
	}

}
