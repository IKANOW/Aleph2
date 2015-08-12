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

}
