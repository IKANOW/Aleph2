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
package com.ikanow.aleph2.core.shared.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ikanow.aleph2.core.shared.utils.DependencyUtils.Edge;
import com.ikanow.aleph2.core.shared.utils.DependencyUtils.Node;

public class TestDependencyUtils {

	@Test
	public void test_topSort_success() {
		new DependencyUtils(); // test coverage!

		{
			final Node seven = new Node("7");
			final Node five = new Node("5");
			final Node five_b = new Node("5");
			final Node seven_b = new Node("7");

			assertFalse(seven.equals(five));
			assertTrue(seven.equals(seven_b));
			
			final Edge e1 = new Edge(five, seven); 
			final Edge e1_b = new Edge(five, seven); 
			final Edge e2 = new Edge(five, seven_b);
			final Edge e3 = new Edge(five_b, seven);
			final Edge e4 = new Edge(five_b, seven_b);
			
			assertFalse(e1.equals(e2)); // (not the same unless it's the same object)
			assertFalse(e1.equals(e3)); 
			assertFalse(e1.equals(e4)); 
			assertTrue(e1_b.equals(e1));
		}
		{
			final Node seven = new Node("7");
			final Node five = new Node("5");
			final Node three = new Node("3");
			final Node eleven = new Node("11");
			final Node eight = new Node("8");
			final Node two = new Node("2");
			final Node nine = new Node("9");
			final Node ten = new Node("10");
		    seven.addEdge(eleven).addEdge(eight);
		    five.addEdge(eleven);
		    three.addEdge(eight).addEdge(ten);
		    eleven.addEdge(two).addEdge(nine).addEdge(ten);
		    eight.addEdge(nine).addEdge(ten);
			
		    final List<Node> in = Arrays.asList(seven, five, three, eleven, eight, two, nine, ten);
		    
		    assertEquals("[3, 5, 7, 11, 2, 8, 9, 10]", Arrays.toString(DependencyUtils.generateOrder(in).success().toArray()));
		}
		// Check the result is deterministic
		{
			final Node seven = new Node("7");
			final Node five = new Node("5");
			final Node three = new Node("3");
			final Node eleven = new Node("11");
			final Node eight = new Node("8");
			final Node two = new Node("2");
			final Node nine = new Node("9");
			final Node ten = new Node("10");
		    seven.addEdge(eleven).addEdge(eight);
		    five.addEdge(eleven);
		    three.addEdge(eight).addEdge(ten);
		    eleven.addEdge(two).addEdge(nine).addEdge(ten);
		    eight.addEdge(nine).addEdge(ten);
			
		    final List<Node> in = Arrays.asList(seven, five, three, eleven, eight, two, nine, ten);
		    
		    assertEquals("[3, 5, 7, 11, 2, 8, 9, 10]", Arrays.toString(DependencyUtils.generateOrder(in).success().toArray()));
		}
	}
	
	@Test
	public void test_topSort_cycle() {
		final Node seven = new Node("7");
		final Node five = new Node("5");
		final Node three = new Node("3");
		final Node eleven = new Node("11");
		final Node eight = new Node("8");
		final Node two = new Node("2");
		final Node nine = new Node("9");
		final Node ten = new Node("10");
	    seven.addEdge(eleven).addEdge(eight);
	    five.addEdge(eleven);
	    three.addEdge(eight).addEdge(ten);
	    eleven.addEdge(two).addEdge(nine).addEdge(ten);
	    eight.addEdge(nine).addEdge(ten).addEdge(seven); // (uh oh, cycle)
		
	    final List<Node> in = Arrays.asList(seven, five, three, eleven, eight, two, nine, ten);
	    
	    assertTrue("Cycle found", DependencyUtils.generateOrder(in).isFail());
	    
	}
	
}
