/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013 Anton Kropp

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 ******************************************************************************/

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
