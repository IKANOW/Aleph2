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
package com.ikanow.aleph2.data_model.utils;

import java.util.Optional;


/** An object container that you can only set once
 *  For cases when you want to make something final but for whatever reason it can't get set
 *  in the c'tor
 * @author acp
 *
 * @param <T>
 */
public class SetOnce<T> {
	
	protected T _t = null;
	
	/** Create the uninitialized SetOnce
	 */
	public SetOnce() {}
	
	/** Create an initialized SetOnce (not sure why you'd ever want to do this?)
	 * @param t - the value to set
	 */
	public SetOnce(final T t) { _t = t; }

	/** Sets the parameter - if it's already set
	 * @param t - the value to set
	 * @return whether the value was set
	 */
	public boolean set(T t) {
		if (null == _t) _t = t;
		return (_t == t); // (ptr ==)
	}
	
	/** Returns an optional containing the value if set
	 * @return an optional containing the value if set
	 */
	public Optional<T> optional() {
		return Optional.ofNullable(_t);
	}
	
	/** Returns the value, throws an unchecked exception if not set
	 * @return
	 */
	public T get() {
		if (null == _t) throw new RuntimeException("Not set");
		return _t;
	}
	
	/** Checks if the value has been set yet
	 * @return whether the setonce has been set
	 */
	public boolean isSet() {
		return _t != null;
	}

	/** A version of "set" that throws an exception if the value is already set instead of failing silently and returning false
	 * @param t - the value to set
	 * @return the value just set
	 */
	public T trySet(T t) {
		if (null != _t) throw new RuntimeException("SetOnce<>: can only set once");
		_t = t;
		return _t;
	}
	
	/** Force override the current value - SHOULD BE USED WITH EXTREME CARE - EG ONLY IN TESTS
	 * @param t - the value to set
	 * @return the value just set
	 */
	@Deprecated
	public T forceSet(T t) {
		_t = t;
		return t;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return optional().toString(); 
	}
}
