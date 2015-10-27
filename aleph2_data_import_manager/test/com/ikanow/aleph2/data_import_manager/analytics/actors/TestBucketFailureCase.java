/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Optional;

import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;

public class TestBucketFailureCase {

	ICrudService<DataBucketBean> _test_crud;	
	
	@Test
	public void test_complexBucket() throws IOException {
		final String json_bucket = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/deser_fail_sample.json"), Charsets.UTF_8);		
		final DataBucketBean bucket = BeanTemplateUtils.from(json_bucket, DataBucketBean.class).get();
		
		MockMongoDbCrudServiceFactory factory = new MockMongoDbCrudServiceFactory();
		_test_crud = factory.getMongoDbCrudService(DataBucketBean.class, String.class, factory.getMongoDbCollection("test.bucket_deser"), Optional.empty(), Optional.empty(), Optional.empty());
		_test_crud.deleteDatastore().join();		
		
		// Read from file
		try {
			java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(baos);
			out.writeObject(bucket);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Errored deseralizing " + BeanTemplateUtils.toJson(bucket));
		}
		// Encapsulate that into bucket
		final BucketActionMessage message = new BucketActionMessage.PollFreqBucketActionMessage(bucket);
		try {
			java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(baos);
			out.writeObject(message);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Errored deseralizing " + BeanTemplateUtils.toJson(message));
		}
		// From DB:
		_test_crud.storeObject(bucket).join();		
		final DataBucketBean bucket2 = _test_crud.getObjectById("deser_fail").join().get();
		try {
			java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(baos);
			out.writeObject(bucket2);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Errored deseralizing " + BeanTemplateUtils.toJson(bucket2));
		}
		// Ah ha .... convert it...
		final DataBucketBean bucket3 = DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(bucket2);
		try {
			java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(baos);
			out.writeObject(bucket3);
			System.out.println(BeanTemplateUtils.toJson(bucket3));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Errored deseralizing " + BeanTemplateUtils.toJson(bucket3));
		}
		
	}
}
