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
package com.ikanow.aleph2.data_model.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class FutureUtils {

	/** A wrapper for a future that enables decoration with a management side channel
	 * @author acp
	 *
	 * @param <T> - the contents of the original wrapper
	 */
	public static class ManagementFutureImpl<T> extends IManagementCrudService.ManagementFuture<T> {

		/**/
		public ManagementFutureImpl(final @NonNull Future<T> delegate, Optional<Future<Collection<BasicMessageBean>>> side_channel) {
			_delegate = delegate;
			_management_side_channel = side_channel;
		}
		
		protected final Future<T> _delegate;
		protected final Optional<Future<Collection<BasicMessageBean>>> _management_side_channel;
		
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			final boolean ret_val = _delegate.cancel(mayInterruptIfRunning)
					&& (!_management_side_channel.isPresent() ? true
							: _management_side_channel.get().cancel(mayInterruptIfRunning));
			
			return ret_val;
		}

		@Override
		public boolean isCancelled() {
			final boolean ret_val = _delegate.isCancelled()
					&& (!_management_side_channel.isPresent() ? true
							: _management_side_channel.get().isCancelled());
			return ret_val;
		}

		@Override
		public boolean isDone() {
			final boolean ret_val = _delegate.isDone()
					&& (!_management_side_channel.isPresent() ? true
							: _management_side_channel.get().isDone());
			return ret_val;
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			return _delegate.get();
		}

		@Override
		public T get(long timeout, TimeUnit unit) throws InterruptedException,
				ExecutionException, TimeoutException {
			return _delegate.get(timeout, unit);
		}

		@Override
		public @NonNull Future<Collection<BasicMessageBean>> getManagementResults() {
			return _management_side_channel.orElse(CompletableFuture.completedFuture(Collections.emptyList()));
		}
		
	}
}
