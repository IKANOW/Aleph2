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
package com.ikanow.aleph2.data_import_manager.harvest.utils;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class TestHarvestErrorUtils {

	static class TestActor extends UntypedActor {
		@Override
		public void onReceive(Object arg0) throws Exception {
		}		
	}
	
	public static class TestMessage {		
	};
	
	static ActorSystem _akka = ActorSystem.create();
	
	
	@Test
	public void testGenerateMessageBean() {

		final ActorRef ref = _akka.actorOf(Props.create(TestActor.class));
		final String ref_name = ref.toString();
		
		final BasicMessageBean test1 = 
				HarvestErrorUtils.buildErrorMessage(ref, new TestMessage(), "TEST ERROR NO PARAMS {0}");
		
		assertEquals(test1.command(), "TestMessage");
		assertEquals((double)test1.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(test1.details(), null);
		assertEquals(test1.message(), "TEST ERROR NO PARAMS {0}");
		assertEquals(test1.message_code(), null);
		assertEquals(test1.source(), ref_name);
		assertEquals(test1.success(), false);
		
		final BasicMessageBean test2 = 
				HarvestErrorUtils.buildErrorMessage(ref, new TestMessage(), HarvestErrorUtils.NO_TECHNOLOGY_NAME_OR_ID, 2L);
		
		assertEquals(test2.command(), "TestMessage");
		assertEquals((double)test2.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(test2.details(), null);
		assertEquals(test2.message(), "No harvest technology name or id in bucket 2");
		assertEquals(test2.message_code(), null);
		assertEquals(test2.source(), ref_name);
		assertEquals(test2.success(), false);
		
		final BasicMessageBean test3 = 
				HarvestErrorUtils.buildErrorMessage(ref, new TestMessage(), HarvestErrorUtils.HARVEST_TECHNOLOGY_NAME_NOT_FOUND, "3a", "3b");
		
		assertEquals(test3.command(), "TestMessage");
		assertEquals((double)test3.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(test3.details(), null);
		assertEquals(test3.message(), "No valid harvest technology 3a found for bucket 3b");
		assertEquals(test3.message_code(), null);
		assertEquals(test3.source(), ref_name);
		assertEquals(test3.success(), false);
		
	}
}
