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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public class PermissionExtractor {
	


	/** 
	 * This class extracts permission values from known classes,e.g._id() or ownerId etc matching permissions.
	 * @return
	 */
	protected String extractOwnerIdentifier(Object object) {
		if (object != null) {
			if (object instanceof SharedLibraryBean) {
				return ((SharedLibraryBean) object).owner_id();
			} else if (object instanceof DataBucketBean) {
				return ((DataBucketBean) object).owner_id();
			} else {
				// try using reflection getting id or _id() or getId()
				try {
					Method m = object.getClass().getMethod("_ownerId");					
					Object retVal = m.invoke(object);
					return ""+retVal;
				} catch (Exception e) {
					// Ignore by default
				}
				try {
					Method m = object.getClass().getMethod("ownerId");					
					Object retVal = m.invoke(object);
						return ""+retVal;
				} catch (Exception e) {
					// Ignore by default
				}
				try {
					Method m = object.getClass().getMethod("getOwnerId");					
					Object retVal = m.invoke(object);
					return ""+retVal;
				} catch (Exception e) {
					// Ignore by default
				}
				try {
					Field f = object.getClass().getDeclaredField("_ownerId");
					f.setAccessible(true);
					Object retVal = f.get(object);
					return ""+retVal;
				} catch (Exception e) {
					// Ignore by default
				}
				try {
					Field f = object.getClass().getDeclaredField("ownerId");
					f.setAccessible(true);
					Object retVal = f.get(object);
					return ""+retVal;
				} catch (Exception e) {
					// Ignore by default
				}
			}
		}
		return null;
	}

	
	/** 
	 * This class extracts permission values from known classes,e.g._id() or fullName or ownerId etc matching permissions.
	 * @return
	 */
	public List<String> extractPermissionIdentifiers(Object object,String action) {
		List<String> permIds =  new ArrayList<String>();
		
		if (object != null) {
			if (object instanceof SharedLibraryBean) {
				permIds.add(createPathPermission(object, action, ((SharedLibraryBean) object).path_name()));
				permIds.add(createPermission(object, action, ((SharedLibraryBean) object)._id()));
				return permIds;
			} else if (object instanceof DataBucketBean) {
				permIds.add(createPathPermission(object, action,((DataBucketBean) object).full_name()));
				permIds.add(createPermission(object, action,((DataBucketBean) object)._id()));
				return permIds;
			} else if (object instanceof DataBucketStatusBean) {
				permIds.add(createPathPermission(object, action,((DataBucketStatusBean) object).bucket_path()));
				permIds.add(createPermission(object, action,((DataBucketStatusBean) object)._id()));
				return permIds;
			} else if (object instanceof String) {
				// adds the object itself
				permIds.add(createPermission(object, action,((String)object)));
				return permIds;
			} else {
				// try using reflection getting id or _id() or getId()
				try {
					Method m = object.getClass().getMethod("_id");					
					Object retVal = m.invoke(object);
					permIds.add(createPermission(object, action,""+retVal));
				} catch (Exception e) {
					// Ignore by default
				}
				try {
					Method m = object.getClass().getMethod("id");					
					Object retVal = m.invoke(object);
					permIds.add(createPermission(object, action,""+retVal));
				} catch (Exception e) {
					// Ignore by default
				}
				try {
					Method m = object.getClass().getMethod("getId");					
					Object retVal = m.invoke(object);
					permIds.add(createPermission(object, action,""+retVal));
				} catch (Exception e) {
					// Ignore by default
				}
				try {
					Field f = object.getClass().getDeclaredField("_id");
					f.setAccessible(true);
					Object retVal = f.get(object);
					permIds.add(createPermission(object, action,""+retVal));
				} catch (Exception e) {
					// Ignore by default
				}
				try {
					Field f = object.getClass().getDeclaredField("id");
					f.setAccessible(true);
					Object retVal = f.get(object);
					permIds.add(createPermission(object, action,""+retVal));
				} catch (Exception e) {
					// Ignore by default
				}
			}
		}
		return permIds;
	}


	public static String createPathPermission(Object permissionRoot, String action, String bucketPath) {		
		String bucketPermission = bucketPath;
		if(bucketPermission!=null ){
			bucketPermission = bucketPermission.replace("/", ":");
			if(bucketPermission.startsWith(":")){
				bucketPermission = bucketPermission.substring(1);
			}
		}
		String prefix = ""; 
		if(action == null){
			action = ISecurityService.ACTION_WILDCARD;
		}
		if(permissionRoot instanceof String){
			prefix = (String)permissionRoot;
		}else{
			prefix = permissionRoot.getClass().getSimpleName();
		}
		return prefix+":"+action+":"+bucketPermission;
	}

	public static String createPermission(Object permissionRoot,String action , String permission) {		
		String prefix = "";
		if(action == null){
			action = ISecurityService.ACTION_WILDCARD;
		}
		if(permissionRoot instanceof String){
			prefix = (String)permissionRoot;
		}else{
			prefix = permissionRoot.getClass().getSimpleName();
		}
		return prefix+":"+action+":"+permission;
	}


}
