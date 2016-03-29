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

/**
 * @author Burch
 *
 */
public class BasicMessageBeanSupplier implements IBasicMessageBeanSupplier {
	final boolean success;
	final Supplier<String> source;
	final Supplier<String> command;
	final Supplier<Integer> message_code;
	final Supplier<String> message;
	final Supplier<Map<String, Object>> details;
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
		this.success = success;
		this.source = source;
		this.command = command;
		this.message_code = message_code;
		this.message = message;
		this.details = details;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier#getBasicMessageBean()
	 */
	@Override
	public BasicMessageBean getBasicMessageBean() {
		return new BasicMessageBean(new Date(), this.success, this.source.get(), this.command.get(), this.message_code.get(), this.message.get(), this.details.get());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier#getSubsystem()
	 */
	@Override
	public String getSubsystem() {
		return this.source.get();
	}

}
