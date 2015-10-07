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
package com.ikanow.aleph2.security.service;

import org.apache.shiro.subject.Subject;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;

public class SubjectWrapper implements ISubject {

	protected Subject subject;
	
	public SubjectWrapper(Object subjectDelegate){
		this.subject = (Subject)subjectDelegate;
	}

	@Override
	public Object getSubject() {
		return subject;
	}

	@Override
	public boolean isAuthenticated() {
		return subject.isAuthenticated();
	}

	@Override
	public String getName() {
		if(subject instanceof Subject){
			return ""+((Subject)subject).getPrincipal();
		}else
			return null;
		
	}

}
