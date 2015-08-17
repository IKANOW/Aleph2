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
package com.ikanow.aleph2.data_model.objects.shared;

import java.io.Serializable;
import java.util.Date;


/** Contains user specific data for authentication.
 * @author joern.freydank@ikanow.com
 */
public class AuthenticationBean implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5696569054090880387L;

	private String _id; // ObJId?
	private String WPUserID;
	private String accountStatus;
	private String accountType;
	private Date created;
	private Date modified;
	private String password;
	private String profileId; // ObjId?
	private String username;
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public String getWPUserID() {
		return WPUserID;
	}
	public void setWPUserID(String wPUserID) {
		WPUserID = wPUserID;
	}
	public String getAccountStatus() {
		return accountStatus;
	}
	public void setAccountStatus(String accountStatus) {
		this.accountStatus = accountStatus;
	}
	public String getAccountType() {
		return accountType;
	}
	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}
	public Date getCreated() {
		return created;
	}
	public void setCreated(Date created) {
		this.created = created;
	}
	public Date getModified() {
		return modified;
	}
	public void setModified(Date modified) {
		this.modified = modified;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getProfileId() {
		return profileId;
	}
	public void setProfileId(String profileId) {
		this.profileId = profileId;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	/* "_id" : ObjectId("4ca4a7c5b94b6296f3468d35"),
    "WPUserID" : null,
    "accountStatus" : "ACTIVE",
    "accountType" : "User",
    "created" : ISODate("2014-12-15T21:55:24.000Z"),
    "modified" : ISODate("2014-12-15T21:55:24.000Z"),
    "password" : "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
    "profileId" : ObjectId("4e3706c48d26852237079004"),
    "username" : "test_user@ikanow.com"
    */
	
}
