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

package com.ikanow.aleph2.distributed_services.utils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.slf4j.MDC;

import akka.dispatch.OnComplete;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

/** Code for converting from Akka future to Java8 completable future
 * @author http://onoffswitch.net/about/
 *
 */
public class AkkaFutureUtils {

	/** Wraps an scala Future returned from Akka in a completable future efficiently using the underlying scala infrastructure
	 * Code from http://onoffswitch.net/converting-akka-scala-futures-java-futures/ 
	 * @param f the scala Future
	 * @param execution_context - the scala execution context - should always be ActorSystem.dispatcher()  
	 * @return the CompletableFuture
	 */
	@SuppressWarnings("unchecked")
	public static <T> CompletableFuture<T> efficientWrap(final scala.concurrent.Future<? extends Object> f, final ExecutionContext execution_context) {
		return (CompletableFuture<T>) new FromScalaFuture<>(f).executeOn(execution_context);
	}	
	
	////////////////////////////////////////////
	
	// SCALA CONVERSION UTILS
	// Code from http://onoffswitch.net/converting-akka-scala-futures-java-futures/
	
	/**Code from http://onoffswitch.net/converting-akka-scala-futures-java-futures/
	 */
	protected static class FromScalaFuture<T> {
		 
	    private final Future<T> future;
	 
	    public FromScalaFuture(Future<T> future) {
	        this.future = future;
	    }
	 
	    public CompletableFuture<T> executeOn(ExecutionContext context) {
	        final CompletableFuture<T> completableFuture = new CompletableFuture<>();
	 
	        final AkkaOnCompleteCallback<T> completer = AkkaCompletionConverter.<T>createCompleter((failure, success) -> {
	            if (failure != null) {
	                completableFuture.completeExceptionally(failure);
	            }
	            else {
	                completableFuture.complete(success);
	            }
	        });
	 
	        future.onComplete(completer.toScalaCallback(), context);
	 
	        return completableFuture;
	    }
	}	
	
	/**Code from http://onoffswitch.net/converting-akka-scala-futures-java-futures/
	 */
	@FunctionalInterface
	protected interface AkkaOnCompleteCallback<T> {
	    OnComplete<T> toScalaCallback();
	}	
	
	/**Code from http://onoffswitch.net/converting-akka-scala-futures-java-futures/
	 */
	protected static class AkkaCompletionConverter {
	    /**
	     * Handles closing over the mdc context map and setting the responding future thread with the
	     * previous context
	     *
	     * @param callback
	     * @return
	     */
	    public static <T> AkkaOnCompleteCallback<T> createCompleter(BiConsumer<Throwable, T> callback) {
	        return () -> {
	 
	            final Map<String, String> oldContextMap = MDC.getCopyOfContextMap();
	 
	            return new OnComplete<T>() {
	                @Override public void onComplete(final Throwable failure, final T success) throws Throwable {
	                    // capture the current threads context map
	                    final Map<String, String> currentThreadsContext = MDC.getCopyOfContextMap();
	 
	                    // set the closed over context map
	                    if(oldContextMap != null) {
	                        MDC.setContextMap(oldContextMap);
	                    }
	 
	                    callback.accept(failure, success);
	 
	                    // return the current threads previous context map
	                    if(currentThreadsContext != null) {
	                        MDC.setContextMap(currentThreadsContext);
	                    }
	                }
	            };
	        };
	    }
	}
		
}
