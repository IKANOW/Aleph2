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
package com.ikanow.aleph2.data_import_manager.harvest.actors;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.data_model.DataImportConfigurationBean;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.core.shared.utils.ClassloaderUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import fj.data.Validation;

/**
 * Created my own DIM test class for node rules
 * so I can constantly change out the dim's config bean and not affect the
 * other tests.
 * 
 * @author Burch
 *
 */
public class TestDataBucketChangeActorNodeRules {

	@Inject 
	protected IServiceContext _service_context = null;
	
	protected DataImportActorContext _actor_context;
	protected ManagementDbActorContext _db_actor_context;
	
	@SuppressWarnings("deprecation")
	public IHarvestTechnologyModule setupDependencies(final Set<String> node_rules) throws Exception {
//		if (null != _service_context) {
//			return;
//		}
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_data_bucket_change.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_db_actor_context = new ManagementDbActorContext(_service_context, true);				
		
		final DataImportConfigurationBean dim_config = BeanTemplateUtils.build(DataImportConfigurationBean.class)
				.with(DataImportConfigurationBean::node_rules, node_rules)
				.done().get();
		
		_actor_context = new DataImportActorContext(_service_context, new GeneralInformationService(), dim_config, null);
		app_injector.injectMembers(_actor_context);
		
		// Have to do this in order for the underlying management db to live...		
		_service_context.getCoreManagementDbService();
		
		// Get the harvest tech module standalone ("in app" way is covered above)		
		final Validation<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleHarvestTechnology", 
						Optional.of(new File(System.getProperty("user.dir") + File.separator + "misc_test_assets" + File.separator + "simple-harvest-example.jar").getAbsoluteFile().toURI().toString()),
						Collections.emptyList(), "test1", "test");						
		
		if (ret_val.isFail()) {
			fail("getHarvestTechnology call failed: " + ret_val.fail().message());
		}
		assertTrue("harvest tech created: ", ret_val.success() != null);
		
		return ret_val.success();
	}		
	
	@Test
	public void testNodeRules() throws Exception {
		//1. no rules, pass
		{
			final IHarvestTechnologyModule harvest_tech = setupDependencies(new HashSet<String>(Arrays.asList()));
			
			final DataBucketBean bucket = createBucketWithRules(Arrays.asList());	
			
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket, null);
			
			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketHarvestChangeActor.talkToHarvester(
					bucket, offer, "test1", _actor_context.getNewHarvestContext(), 
					Validation.success(harvest_tech), _actor_context);		
			
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test1.get().getClass());
		}
		
		//2. inclusive single hostname
		{			
			final String hostname = _actor_context.getInformationService().getHostname(); //this is my hostname for comparing globs/regex to
			final IHarvestTechnologyModule harvest_tech = setupDependencies(new HashSet<String>(Arrays.asList("host1.aaa")));
			final DataBucketBean bucket = createBucketWithRules(Arrays.asList(hostname));	
			
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket, null);
			
			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketHarvestChangeActor.talkToHarvester(
					bucket, offer, "test1", _actor_context.getNewHarvestContext(), 
					Validation.success(harvest_tech), _actor_context);		
			
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test1.get().getClass());
		}
		
		//3. inclusive single rule
		{
			final IHarvestTechnologyModule harvest_tech = setupDependencies(new HashSet<String>(Arrays.asList("host1.aaa")));
			final DataBucketBean bucket = createBucketWithRules(Arrays.asList("$host1*"));	
			
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket, null);
			
			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketHarvestChangeActor.talkToHarvester(
					bucket, offer, "test1", _actor_context.getNewHarvestContext(), 
					Validation.success(harvest_tech), _actor_context);		
			
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test1.get().getClass());
		}
		
		//4. exclusive single hostname
		{
			final IHarvestTechnologyModule harvest_tech = setupDependencies(new HashSet<String>(Arrays.asList("host1.aaa")));
			final DataBucketBean bucket = createBucketWithRules(Arrays.asList("-not_this_host"));	
			
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket, null);
			
			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketHarvestChangeActor.talkToHarvester(
					bucket, offer, "test1", _actor_context.getNewHarvestContext(), 
					Validation.success(harvest_tech), _actor_context);		
			
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test1.get().getClass());
		}
		
		//5. exclusive single rule
		{
			final IHarvestTechnologyModule harvest_tech = setupDependencies(new HashSet<String>(Arrays.asList("host2.aaa")));
			final DataBucketBean bucket = createBucketWithRules(Arrays.asList("-$host1*"));	
			
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket, null);
			
			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketHarvestChangeActor.talkToHarvester(
					bucket, offer, "test1", _actor_context.getNewHarvestContext(), 
					Validation.success(harvest_tech), _actor_context);		
			
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test1.get().getClass());
		}
		
		//6. default (inclusive) multiple rules w/ misses
		{
			final IHarvestTechnologyModule harvest_tech = setupDependencies(new HashSet<String>(Arrays.asList("host2.aaa")));
			final DataBucketBean bucket = createBucketWithRules(Arrays.asList("$host1*", "$host2*", "$host3*"));	
			
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket, null);
			
			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketHarvestChangeActor.talkToHarvester(
					bucket, offer, "test1", _actor_context.getNewHarvestContext(), 
					Validation.success(harvest_tech), _actor_context);		
			
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test1.get().getClass());
		}
		
		//6. default (inclusive) multiple hostname w/ misses
		{
			final String hostname = _actor_context.getInformationService().getHostname(); //this is my hostname for comparing globs/regex to
			final IHarvestTechnologyModule harvest_tech = setupDependencies(new HashSet<String>(Arrays.asList("host2.aaa")));
			final DataBucketBean bucket = createBucketWithRules(Arrays.asList("host1*", "host2*", "host3*", hostname));	
			
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket, null);
			
			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketHarvestChangeActor.talkToHarvester(
					bucket, offer, "test1", _actor_context.getNewHarvestContext(), 
					Validation.success(harvest_tech), _actor_context);		
			
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test1.get().getClass());
		}
		
		//7. default (inclusive) multiple rule/hostname w/ misses
		{
			final String hostname = _actor_context.getInformationService().getHostname(); //this is my hostname for comparing globs/regex to
			final IHarvestTechnologyModule harvest_tech = setupDependencies(new HashSet<String>(Arrays.asList("host2.aaa")));
			final DataBucketBean bucket = createBucketWithRules(Arrays.asList("$host1*", "$host2*", "$host3*", "host1*", "host2*", "host3*", hostname));	
			
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket, null);
			
			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketHarvestChangeActor.talkToHarvester(
					bucket, offer, "test1", _actor_context.getNewHarvestContext(), 
					Validation.success(harvest_tech), _actor_context);		
			
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test1.get().getClass());
		}
		
		//8. multiple rules, all misses, fail
		{
			final IHarvestTechnologyModule harvest_tech = setupDependencies(new HashSet<String>(Arrays.asList("host5.aaa")));
			final DataBucketBean bucket = createBucketWithRules(Arrays.asList("$host1*", "$host2*", "$host3*", "host1*", "host2*", "host3*"));	
			
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket, null);
			
			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketHarvestChangeActor.talkToHarvester(
					bucket, offer, "test1", _actor_context.getNewHarvestContext(), 
					Validation.success(harvest_tech), _actor_context);		
			
			assertEquals(BucketActionReplyMessage.BucketActionIgnoredMessage.class, test1.get().getClass());
		}
	}
	
	protected DataBucketBean createBucketWithRules(final List<String> rules) {
		return BeanTemplateUtils.build(DataBucketBean.class)
							.with(DataBucketBean::_id, "test1")
							.with(DataBucketBean::owner_id, "person_id")
							.with(DataBucketBean::full_name, "/test/path/")
							.with(DataBucketBean::node_list_rules, rules)
							.done().get();
	}
}
