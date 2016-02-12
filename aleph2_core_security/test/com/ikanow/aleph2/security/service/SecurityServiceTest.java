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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean.LibraryType;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.security.utils.ErrorUtils;
import com.ikanow.aleph2.security.utils.ProfilingUtility;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;


public class SecurityServiceTest {
	private static final Logger logger = LogManager.getLogger(SecurityServiceTest.class);

	protected Config config = null;
	

	@Inject
	protected IServiceContext _temp_service_context = null;
	protected static IServiceContext _service_context = null;

	//TODO: move this back to ISecService
	protected SecurityService securityService = null;
	
	protected String regularUserId = "user";

	@Before
	public void setupDependencies() throws Exception {
		try {
			if (null == _service_context) {

				final String temp_dir = System.getProperty("java.io.tmpdir");

				// OK we're going to use guice, it was too painful doing this by
				config = ConfigFactory
						.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_core_security.properties")))
						.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
						.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
						.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
						.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));

				Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));
				app_injector.injectMembers(this);
				_service_context = _temp_service_context;
			}
			this.securityService = (SecurityService)_service_context.getSecurityService();
			
			SecurityService.systemPassword = "admin123";
			SecurityService.systemUsername = "admin";
			
		} catch (Throwable e) {
			logger.error(ErrorUtils.getLongForm(ErrorUtils.EXCEPTION_CAUGHT, e));

		}

	}

	@Test
	public void testAuthenticated() {
		ISubject subject = loginAsTestUser();
        try {
		} catch (AuthenticationException e) {
			logger.info("Caught (expected) Authentication exception:"+e.getMessage());
		}	
		
		assertEquals(true, subject.isAuthenticated());
		logger.info("Authentication successful for "+subject.getName());
		
	}
	
	@Test
	public void testRole_newAPI(){
		ISubject subject = securityService.getSystemUserContext();
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.hasRole(subject,"admin"));
	}
	@Test
	public void testRole(){
		ISubject subject = loginAsAdmin();
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.hasRole(subject,"admin"));
	}

	@Test
	public void testPermission_newAPI(){
		ISubject subject = securityService.getUserContext("user");
		String permission = "permission1";
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,permission));
	}
	@Test
	public void testPermission(){
		ISubject subject = loginAsRegularUser();
		String permission = "permission1";
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,permission));
	}

	@Test
	public void testRunAs_newAPI(){
		// system community
		String runAsPrincipal = "user";
		String runAsRole = "user";
		String runAsPersonalPermission = "permission1";
		String runAsPersonalPermission_not = "NOT_permission1";

		ISubject subject = securityService.getUserContext(runAsPrincipal);
		assertEquals(true, subject.isAuthenticated());
				
		assertEquals(true,securityService.hasRole(subject,runAsRole));
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,runAsPersonalPermission));
		assertEquals(false,securityService.isPermitted(subject,runAsPersonalPermission_not));
	}
	@Test
	public void testRunAs(){
		ISubject subject = loginAsTestUser();
		// system community
		String runAsPrincipal = "user";
		String runAsRole = "user";
		String runAsPersonalPermission = "permission1";
		
		securityService.runAs(subject,Arrays.asList(runAsPrincipal));
		
		assertEquals(true,securityService.hasRole(subject,runAsRole));
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,runAsPersonalPermission));
		Collection<String> p = securityService.releaseRunAs(subject);
		logger.debug("Released Principals:"+p);
		securityService.runAs(subject,Arrays.asList(runAsPrincipal));
		
		assertEquals(true,securityService.hasRole(subject,runAsRole));
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,runAsPersonalPermission));
		p = securityService.releaseRunAs(subject);
		logger.debug("Released Principals:"+p);
	}
	
	protected ISubject loginAsTestUser() throws AuthenticationException{
		ISubject subject = securityService.login("testUser","testUser123");			
		return subject;
	}

	protected ISubject loginAsAdmin() throws AuthenticationException{
		ISubject subject = securityService.login("admin","admin123");			
		return subject;
	}

	protected ISubject loginAsRegularUser() throws AuthenticationException{
		ISubject subject = securityService.login("user","user123");			
		return subject;
	}
	
	@Test
	public void testCaching(){
		ISubject subject = loginAsRegularUser();
		// test personal community permission
		String permission = "permission2";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("TU-permisssion0");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU-permisssion"+(i+1));
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("TU-permisssion"+(i+1));
		}
		subject = loginAsAdmin();
		// test personal community permission
		permission = "permission2";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("AU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("AU-permisssion");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("AU-permisssion"+i+1);
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("AU-permisssion"+i+1);
		}
		
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU2-permisssion_L"+(i+1));
		subject = loginAsRegularUser();
		ProfilingUtility.timeStopAndLog("TU2-permisssion_L"+(i+1));
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU2-permisssion"+(i+1));
		assertEquals(true,securityService.isPermitted(subject,permission));
			ProfilingUtility.timeStopAndLog("TU2-permisssion"+(i+1));
		}
	}

	@Test
	public void testInvalidateAuthenticationCache(){
		ISubject subject = loginAsTestUser();
		
		securityService.runAs(subject,Arrays.asList(regularUserId));

		// test personal community permission
		String permission = "permission1";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("TU-permisssion0");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU-permisssion"+(i+1));
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("TU-permisssion"+(i+1));
			if(i==5){
				securityService.invalidateAuthenticationCache(Arrays.asList(regularUserId));	
				//loginAsRegularUser();
			}
		}
	}

	@Test
	public void testInvalidateCache(){
		ISubject subject = loginAsTestUser();
		
		securityService.runAs(subject,Arrays.asList(regularUserId));

		// test personal community permission
		String permission = "permission1";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("TU-permisssion0");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU-permisssion"+(i+1));
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("TU-permisssion"+(i+1));
			if(i==5){
				securityService.invalidateCache();	
				//loginAsRegularUser();
			}
		}
	}

	@Test
	public void testRunAsDemoted_newAPI(){
		ISubject subject = securityService.getUserContext(regularUserId);	
		ISubject other_subject = securityService.getSystemUserContext();	
		assertEquals(false,securityService.hasRole(subject,"admin"));
		assertEquals(true,securityService.hasRole(other_subject,"admin"));
	}
	@Test
	public void testRunAsDemoted(){
		ISubject subject = loginAsAdmin();
		// system community
		String runAsPrincipal = regularUserId; 
		
		assertEquals(true,securityService.hasRole(subject,"admin"));

		securityService.runAs(subject,Arrays.asList(runAsPrincipal));
		
		assertEquals(false,securityService.hasRole(subject,"admin"));
		Collection<String> p = securityService.releaseRunAs(subject);
		assertEquals(true,securityService.hasRole(subject,"admin"));
		logger.debug("Released Principals:"+p);
	}

	@Test
	public void testJvmSecurity(){
		@SuppressWarnings("unused")
		ISubject subject = loginAsRegularUser();
		securityService.enableJvmSecurityManager(true);
		File f =  new File("/tmp/data/misc");
		f.list();
		try {
			f =  new File("/tmp/data/");
			f.list();
			fail("Read Access to "+f.getName()+" is not allowed.");
		} catch (Throwable t) {			
			logger.debug("This is correct we want to see a read a security exception here:"+t);
		}
		finally {
			securityService.enableJvmSecurityManager(false);
		} 
	}

	@Test
	@Ignore
	public void testSecurityServiceMultiThreading(){
		ISubject subject = securityService.loginAsSystem();
		long timeNow = System.currentTimeMillis();
		long timeOut = timeNow+3000L;
		boolean hasAdminRole = false;
		while(timeNow < timeOut){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			hasAdminRole  = securityService.hasRole(subject, "admin");
			if(hasAdminRole){
				fail("Subject received admin rights");
				break;
			}
			timeNow = System.currentTimeMillis();
		} // while
	
	}


	
	@Test
	public void testSystemLogin() {
		ISubject subject = securityService.loginAsSystem();			
		securityService.releaseRunAs(subject);		
		securityService.runAs(subject, Arrays.asList("testUser"));
		
	}
	
	@Test
	public void testSecuredCrudManagementService() throws Exception{
		 AuthorizationBean authBean = new AuthorizationBean("admin");
		
		IDummyCrudService mockedDelegate = mock(IDummyCrudService.class);
		
		
		CompletableFuture<Optional<String>> future = CompletableFuture.completedFuture(Optional.of("abc"));
		ManagementFuture<Optional<String>> managedFuture =  FutureUtils.createManagementFuture(future);
		when(mockedDelegate.getObjectBySpec(any())).thenReturn(managedFuture);
		when(mockedDelegate.getObjectById(any())).thenReturn(managedFuture);
		when(mockedDelegate.getFilteredRepo(any(),any(),any())).thenReturn(mockedDelegate);
		
		Cursor<String> cursor = new Cursor<String>(){
			private List<String> l = Arrays.asList("a","b","c");
			private Iterator<String> iterator = l.iterator();
			@Override
			public Iterator<String> iterator() {
				return iterator;
			}

			@Override
			public void close() throws Exception {				
				
			}

			@Override
			public long count() {
				return l.size();
			}
			
		};
		CompletableFuture<Cursor<String>> future2 = CompletableFuture.completedFuture(cursor);
		ManagementFuture<Cursor<String>> result2 =  FutureUtils.createManagementFuture(future2);
		when(mockedDelegate.getObjectsBySpec(any())).thenReturn(result2);		
		when(mockedDelegate.getObjectsBySpec(any(),(List<String>) anyCollectionOf(String.class),anyBoolean())).thenReturn(result2);
		
		IManagementCrudService<String> securedCrudService = securityService.secured(mockedDelegate, authBean);
		//DummySecuredCrudService securedCrudService = new DummySecuredCrudService(_service_context, mockedDelegate, authBean);
		((SecuredCrudManagementDbService<String>)securedCrudService).setPermissionExtractor(((SecuredCrudManagementDbService<String>)securedCrudService).getPermissionExtractor());
		securedCrudService.storeObject("abc");
		securedCrudService.storeObject("abc",true);
		securedCrudService.storeObjects(Arrays.asList("abc","efg"));
		securedCrudService.storeObjects(Arrays.asList("abc","efg"),false);
		securedCrudService.optimizeQuery(Arrays.asList("f"));
		SingleQueryComponent<String> query = CrudUtils.allOf(String.class);
		securedCrudService.getObjectBySpec(query);
		securedCrudService.getObjectBySpec(query,Arrays.asList("f"),true);
		securedCrudService.getObjectById(1);
		securedCrudService.getObjectById(1,Arrays.asList("f"),true);
		securedCrudService.optimizeQuery(Arrays.asList("f"));
		securedCrudService.deregisterOptimizedQuery(Arrays.asList("f"));
		securedCrudService.getFilteredRepo("a",Optional.empty(),Optional.empty());
		ManagementFuture<Cursor<String>> result3 = securedCrudService.getObjectsBySpec(query);
		Cursor<String> c3 = result3.get();
		while(c3.iterator().hasNext()){
			c3.iterator().next();
		}
		c3.count();
		c3.close();
		securedCrudService.getObjectsBySpec(query,Arrays.asList("f"),true);
		securedCrudService.countObjectsBySpec(query);
		securedCrudService.countObjects();
		UpdateComponent<String> uc = CrudUtils.update(String.class);
		securedCrudService.updateObjectById(1, uc);
		securedCrudService.updateObjectBySpec(query,Optional.empty(),uc);
		securedCrudService.updateObjectsBySpec(query,Optional.empty(),uc);
		securedCrudService.updateAndReturnObjectBySpec(query,Optional.empty(),uc,Optional.empty(),Arrays.asList("f"),true);
		securedCrudService.deleteObjectById(1);
		securedCrudService.deleteObjectBySpec(query);
		securedCrudService.deleteObjectsBySpec(query);
		securedCrudService.deleteDatastore();
		securedCrudService.getRawService();
		securedCrudService.getSearchService();
		securedCrudService.getUnderlyingPlatformDriver(this.getClass(),Optional.empty());
		securedCrudService.getCrudService();
		
		}
		
	@SuppressWarnings({ "rawtypes", "unchecked", "unused"})
	@Test
	public void testPermissionExtractor(){
		PermissionExtractor extractor = new PermissionExtractor();
		SharedLibraryBean libraryBean = new SharedLibraryBean("1", "display_name", "/tmp/bucket1",
				LibraryType.misc_archive, "subtype", "owner_id",
				new HashSet(), 
				"batch_streaming_entry_point","batch_enrichment_entry_point"," misc_entry_point",
				new HashMap());
		assertNotNull(extractor.extractPermissionIdentifiers(libraryBean,Optional.empty()));
		
		DataBucketBean dataBucketBean = mock(DataBucketBean.class);
		when(dataBucketBean._id()).thenReturn("1");
		when(dataBucketBean.owner_id()).thenReturn("99");
		assertNotNull(extractor.extractPermissionIdentifiers(dataBucketBean,Optional.empty()));
		assertEquals(2, extractor.extractPermissionIdentifiers(dataBucketBean,Optional.empty()).size());
		DataBucketStatusBean dataBucketStatusBean = mock(DataBucketStatusBean.class);
		when(dataBucketStatusBean._id()).thenReturn("2");
		
		assertNotNull(extractor.extractPermissionIdentifiers(
				new TestWithId(){
			public String _id(){
				return "1";
			};
		},Optional.empty()));
		assertNotNull(extractor.extractPermissionIdentifiers(
				new TestWithId(){
			public String id(){
				return "2";
			};
		},Optional.empty()));
		assertNotNull(extractor.extractPermissionIdentifiers(
				new TestWithId(){
			public String getId(){
				return "3";
			};
		},Optional.empty()));
		assertNotNull(extractor.extractPermissionIdentifiers(
				new TestWithId(){
					protected String _id = "4";
					
		},Optional.empty()));		
		assertNotNull(extractor.extractPermissionIdentifiers(new TestWithId(),Optional.empty()));
		
		assertArrayEquals(new String[]{"community:*:comunityId1"},extractor.extractPermissionIdentifiers(Tuples._2T("community", "comunityId1"),Optional.empty()).toArray());
		
		// tesr ownerId extractions
		assertNotNull(extractor.extractOwnerIdentifier(libraryBean));
		assertNotNull(extractor.extractOwnerIdentifier(dataBucketBean));
		assertNotNull(extractor.extractOwnerIdentifier(
				new TestWithOwnerId(){
			public String _ownerId(){
				return "91";
			};
		}));
		assertNotNull(extractor.extractOwnerIdentifier(
				new TestWithOwnerId(){
			public String ownerId(){
				return "92";
			};
		}));
		assertNotNull(extractor.extractOwnerIdentifier(
				new TestWithOwnerId(){
			public String getOwnerId(){
				return "93";
			};
		}));
		assertNotNull(extractor.extractOwnerIdentifier(
				new TestWithOwnerId(){
					protected String _ownerId = "94";
					
		}));		
		assertNotNull(extractor.extractOwnerIdentifier(new TestWithOwnerId()));
				
	}
	
	private class TestWithId {
		protected String id = "5";
	}
	private class TestWithOwnerId {
		protected String ownerId = "95";
	}

	@Test
	public void testMisc() throws Exception{
		((SecurityService)securityService).setSessionTimeout(900000);
		assertNotNull(SecurityService.getExtraDependencyModules());
		((SecurityService)securityService).youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules();
		securityService.getUnderlyingArtefacts();
		securityService.getUnderlyingPlatformDriver(this.getClass(), Optional.empty());				
	}
	
	@Test 
	public void testUserAccess_newAPI() throws Exception{
		// test read access
     	AuthorizationBean authBean = new AuthorizationBean("user");		
     	MockSecuredCrudServiceBean mockedDelegate = mock(MockSecuredCrudServiceBean.class);
		SingleQueryComponent<DataBucketBean> query = CrudUtils.allOf(DataBucketBean.class);

		DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/allowed")
				.with(DataBucketBean::_id, "bucketId1")
			.done().get();

		IManagementCrudService<DataBucketBean> securedCrudService = securityService.secured(mockedDelegate, authBean);

		CompletableFuture<Optional<DataBucketBean>> future = CompletableFuture.completedFuture(Optional.of(bucket));
		ManagementFuture<Optional<DataBucketBean>> managedFuture =  FutureUtils.createManagementFuture(future);
		when(mockedDelegate.getObjectBySpec(any())).thenReturn(managedFuture);
		when(mockedDelegate.getObjectById(any())).thenReturn(managedFuture);

		ManagementFuture<Optional<DataBucketBean>> f = securedCrudService.getObjectBySpec(query);
		Optional<DataBucketBean> o = f.get();
		DataBucketBean b = o.get();
		assertNotNull(b);
		
		
		// test read with a different user
		ISubject subject1 = securityService.getUserContext("testUser");
		ISubject subject2 = securityService.getUserContext("user");
		
		boolean permitted = securityService.isUserPermitted(subject1, bucket, Optional.of("read"));
		assertFalse(permitted);
		// test write with a user who has (only) read permissions
		permitted = securityService.isUserPermitted(subject2, bucket, Optional.of("write"));
		assertFalse(permitted);
		// test read  by logging in as system but testing with userId permissions
		assertTrue(securityService.isUserPermitted(subject2, bucket, Optional.of("read")));
		assertFalse(securityService.isUserPermitted(subject1, bucket, Optional.of("read")));
		assertFalse(securityService.isUserPermitted(subject1, "community:read:communityId1", Optional.empty()));
		assertTrue(securityService.isUserPermitted(subject2, "community:read:communityId1", Optional.empty()));

	}
	@Test 
	public void testUserAccess() throws Exception{
		// test read access
     	AuthorizationBean authBean = new AuthorizationBean("user");		
     	MockSecuredCrudServiceBean mockedDelegate = mock(MockSecuredCrudServiceBean.class);
		SingleQueryComponent<DataBucketBean> query = CrudUtils.allOf(DataBucketBean.class);

		DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/allowed")
				.with(DataBucketBean::_id, "bucketId1")
			.done().get();

		IManagementCrudService<DataBucketBean> securedCrudService = securityService.secured(mockedDelegate, authBean);

		CompletableFuture<Optional<DataBucketBean>> future = CompletableFuture.completedFuture(Optional.of(bucket));
		ManagementFuture<Optional<DataBucketBean>> managedFuture =  FutureUtils.createManagementFuture(future);
		when(mockedDelegate.getObjectBySpec(any())).thenReturn(managedFuture);
		when(mockedDelegate.getObjectById(any())).thenReturn(managedFuture);

		ManagementFuture<Optional<DataBucketBean>> f = securedCrudService.getObjectBySpec(query);
		Optional<DataBucketBean> o = f.get();
		DataBucketBean b = o.get();
		assertNotNull(b);
		// test read by being logged in from before		
		boolean permitted = securityService.isUserPermitted(Optional.empty(), bucket, Optional.of("read"));
		assertTrue(permitted);
		// test read with a different user
		permitted = securityService.isUserPermitted(Optional.of("testUser"), bucket, Optional.of("read"));
		assertFalse(permitted);
		// test write with a user who has (only) read permissions
		permitted = securityService.isUserPermitted(Optional.of("user"), bucket, Optional.of("write"));
		assertFalse(permitted);
		// test read  by logging in as system but testing with userId permissions
		securityService.loginAsSystem();
		permitted = securityService.isUserPermitted(Optional.of("user"), bucket, Optional.of("read"));
		assertTrue(permitted);
		permitted = securityService.isUserPermitted(Optional.of("user"), "community:read:communityId1", Optional.empty());
		assertTrue(permitted);

	}
}
