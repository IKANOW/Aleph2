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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import fj.data.Validation;

/** Provides some simple utils to handle dependency order
 *  Topological sort implementation taken from http://stackoverflow.com/a/2739768
 *  (based on Kahn's algorithm https://en.wikipedia.org/wiki/Topological_sorting)
 * @author Alex
 */
public class DependencyUtils {

	protected static Validation<String, List<Node>> generateOrder(final List<Node> mutable_in) {
		//L <- Empty list that will contain the sorted elements
		ArrayList<Node> L = new ArrayList<Node>();

		//S <- Set of all nodes with no incoming edges
		HashSet<Node> S = new HashSet<Node>(); 
		for(Node n : mutable_in){
			if(n.inEdges.size() == 0){
				S.add(n);
			}
		}

		//while S is non-empty do
		while(!S.isEmpty()){
			//DEBUG
			//System.out.println("CYCLE: " + S.toString());
			
			//remove a node n from S
			Node n = S.iterator().next();
			S.remove(n);

			//insert n into L
			L.add(n);

			//for each node m with an edge e from n to m do
			for(Iterator<Edge> it = n.outEdges.iterator();it.hasNext();){
				//remove edge e from the graph
				Edge e = it.next();
				Node m = e.to;
				it.remove();//Remove edge from n
				m.inEdges.remove(e);//Remove edge from m

				//if m has no other incoming edges then insert m into S
				if(m.inEdges.isEmpty()){
					S.add(m);
				}
			}
		}
		//Check to see if all edges are removed
		boolean cycle = false;
		for(Node n : mutable_in){
			if(!n.inEdges.isEmpty()){
				cycle = true;
				break;
			}
		}
		return cycle
				? Validation.fail("Cycle present")
				: Validation.success(L)
				;
	}

	protected static class Node{
		public final String name;
		public final HashSet<Edge> inEdges;
		public final HashSet<Edge> outEdges;
		public Node(String name) {
			this.name = name;
			inEdges = new HashSet<Edge>();
			outEdges = new HashSet<Edge>();
		}
		public Node addEdge(Node node){
			Edge e = new Edge(this, node);
			outEdges.add(e);
			node.inEdges.add(e);
			return this;
		}
		@Override
		public String toString() {
			return name;
		}
		@Override
		public int hashCode() {
			return name.hashCode();
		}
		@Override
		public boolean equals(Object obj) {
			Node n = (Node)obj;
			return this.name.equals(n.name);
		}
	}

	protected static class Edge{
		public final Node from;
		public final Node to;
		public Edge(Node from, Node to) {
			this.from = from;
			this.to = to;
		}
		@Override
		public boolean equals(Object obj) {
			Edge e = (Edge)obj;
			return e.from == from && e.to == to;
		}
	}
}
