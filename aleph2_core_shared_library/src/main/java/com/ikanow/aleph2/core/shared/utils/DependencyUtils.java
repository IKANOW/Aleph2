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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;
import scala.Tuple3;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.UuidUtils;

import fj.P2;
import fj.data.State;
import fj.data.Validation;

/** Provides some simple utils to handle dependency order
 *  Topological sort implementation taken from http://stackoverflow.com/a/2739768
 *  (based on Kahn's algorithm https://en.wikipedia.org/wiki/Topological_sorting)
 * @author Alex
 */
public class DependencyUtils {

	/////////////////////

	// PUBLIC API
	
	/** Breaks the list of enrichment elements into containers that need to be separated by expensive serialization/distribution boundaries
	 * @param in
	 * @return
	 */
	public static Validation<String, LinkedHashMap<String, Tuple2<Set<String>, List<EnrichmentControlMetadataBean>>>> buildPipelineOfContainers(final Collection<String> inputs, final List<EnrichmentControlMetadataBean> in) {
		
		final List<EnrichmentControlMetadataBean> tidy = tidyUpEnrichmentPipeline(in);

		// Create a set of all the available inputs that are also referenced in the enrichment pipeline
		final Set<String> refd_inputs = tidy.stream().flatMap(e -> e.dependencies().stream()).collect(Collectors.toSet());
		final Collection<String> filtered_inputs =
				Stream.concat(inputs.stream(), Stream.of(EnrichmentControlMetadataBean.PREVIOUS_STEP_ALL_INPUTS))
						.filter(i -> refd_inputs.contains(i))
						.collect(Collectors.toSet())
						;
		
		return generateOrder(createDependencyGraph(filtered_inputs, tidy))
				.<LinkedHashMap<String, Tuple2<Set<String>, List<EnrichmentControlMetadataBean>>>>map(dep_graph -> 
						buildPipelineOfContainers(
							dep_graph,
							tidy.stream().collect(Collectors.toMap(e -> e.name(), e -> e))
							)
				);
	}
	
	/** Tidies up the pipeline elements by replacing $previous (null maps to $previous, if first element, $previous maps to $all_inputs)
	 *  (this isn't really public API but I'm leaving it public in case other simpler enrichment engines want to use it)
	 * @param in the list of beans
	 * @return
	 */
	public static List<EnrichmentControlMetadataBean> tidyUpEnrichmentPipeline(final List<EnrichmentControlMetadataBean> in) {
		
		// Build a stream of previous/next, starting at (null, first)
		return StreamUtils.<Tuple3<EnrichmentControlMetadataBean, EnrichmentControlMetadataBean, Iterator<EnrichmentControlMetadataBean>>>
			unfold(Tuples._3T(null, null, in.iterator()), prev_curr_it -> {
				if (prev_curr_it._3().hasNext()) {
					return Optional.of(Tuples._3T(prev_curr_it._2(), prev_curr_it._3().next(), prev_curr_it._3()));
				}
				else return Optional.empty();
			})
			.filter(t3 -> null != t3._2()) // (first element is "before the start", so discard that)
			.<Tuple2<EnrichmentControlMetadataBean, EnrichmentControlMetadataBean>>map(t3-> Tuples._2T(t3._1(), t3._2()))
			.map(prev_next -> {
				final String previous_name = Optionals.of(() -> prev_next._1().name()).orElse(EnrichmentControlMetadataBean.PREVIOUS_STEP_ALL_INPUTS);
				
				return BeanTemplateUtils.clone(prev_next._2())
						.with(EnrichmentControlMetadataBean::name, Optional.ofNullable(prev_next._2().name()).orElseGet(() -> UuidUtils.get().getRandomUuid()))
						.with(EnrichmentControlMetadataBean::dependencies, 
								Patterns.match(prev_next._2().dependencies()).<List<String>>andReturn()
										.when(l -> null == l, __ -> Arrays.asList(EnrichmentControlMetadataBean.PREVIOUS_STEP_ALL_INPUTS))
										.otherwise(l -> l.stream().map(s -> s.equals(EnrichmentControlMetadataBean.PREVIOUS_STEP_DEPENDENCY) ? previous_name : s)
															.collect(Collectors.toList()))
								)
						.done();
			})
			.collect(Collectors.toList())
			;
	}

	/////////////////////

	// COMBO GRAPH/ALEPH2 UTILS
	
	/** Breaks the list of enrichment elements into containers that need to be separated by expensive serialization/distribution boundaries
	 * @param ordered_pipeline
	 * @param pipeline_elements
	 * @return
	 */
	protected static LinkedHashMap<String, Tuple2<Set<String>, List<EnrichmentControlMetadataBean>>> buildPipelineOfContainers(final List<Node> ordered_pipeline, final Map<String, EnrichmentControlMetadataBean> pipeline_elements)
	{
		final LinkedHashMap<String, Node> node_lookup = ordered_pipeline.stream().collect(Collectors.toMap(v -> v.name, v -> v, (a1, a2) -> a1, () -> new LinkedHashMap<>()));
		return ordered_pipeline.stream()
			.reduce(
					new LinkedHashMap<String, Tuple2<Set<String>, List<EnrichmentControlMetadataBean>>>(),
					(acc, v) -> {
						final Optional<EnrichmentControlMetadataBean> maybe_enricher = Optional.ofNullable(pipeline_elements.get(v.name));
						return maybe_enricher.<Tuple2<Set<String>, List<EnrichmentControlMetadataBean>>>flatMap(enricher -> 
							Optional.ofNullable(Lambdas.get(() -> {
								if (enricher.dependencies().size() == 1) {
									return acc.get(enricher.dependencies().get(0));
								}
								else {  // 0, 2+ .. always create a new component
									return null;
								}
							}))
							.map(curr_state -> {
								
								// OK here's some other cases (in addition to "fan in" above) where we need to create a new container:
								// 0) the current container is an input
								if (curr_state._1().isEmpty()) {
									return null;
								}
								// 1) i have grouping:
								else if (Optionals.ofNullable(enricher.grouping_fields()).size() > 0) {
									return null;
								}
								// 2) I am one of the forks of a fan out (the fan out node itself is the last element)								
								else if (Lambdas.get(() -> {
									final String sole_dep = enricher.dependencies().get(0); // exists by construction)
									final Optional<Node> maybe_parent_node = Optional.ofNullable(node_lookup.get(sole_dep)); // (exists by construction)
									return maybe_parent_node.map(parent_node -> parent_node.numOutEdges > 1).orElse(true);
								})) {
									return null;
								}
								else return curr_state;
							})
							.map(curr_state -> { // if still here then add the element
								curr_state._2().add(enricher);
								return curr_state;
							})
						)
						.map(Optional::of)
						.orElseGet(() -> { //(create a new component)							
							final Tuple2<Set<String>, List<EnrichmentControlMetadataBean>> new_state = 
									maybe_enricher
										.map(enricher -> Tuples._2T((Set<String>)ImmutableSet.copyOf(enricher.dependencies()), (List<EnrichmentControlMetadataBean>)new LinkedList<>(Arrays.asList(enricher))))
										.orElseGet(() -> Tuples._2T(Collections.<String>emptySet(), (List<EnrichmentControlMetadataBean>)new LinkedList<EnrichmentControlMetadataBean>()))
									;
							return Optional.of(new_state);
						})
						.map(state -> {
							acc.put(v.name, state);								
							return acc;
						})
						.get()
						;
					}, 
					(acc1, acc2) -> acc1
					)
					;
	}
	
	/** Creates a list of nodes with the edges wired up
	 * @param inputs
	 * @param enrichment_elements
	 * @return
	 */
	protected static List<Node> createDependencyGraph(final Collection<String> inputs, List<EnrichmentControlMetadataBean> enrichment_elements) {
		
		final Function<String, State<Map<String, Node>, Node>> addInput = i -> {
			return State.<Map<String, Node>, Node>unit(map -> {
				final Node n = new Node(i);
				map.put(i, n);				
				return PTuple2.of(map, n);
			});
			
		};
		final Function<EnrichmentControlMetadataBean, State<Map<String, Node>, Node>> addEnrichment = e -> {
			return State.<Map<String, Node>, Node>unit(map -> {
				final Node this_node = map.computeIfAbsent(e.name(), k -> new Node(k));
				Optionals.ofNullable(e.dependencies()).stream().forEach(dep -> {
					final Node dep_node = map.computeIfAbsent(dep, k -> new Node(k));
					dep_node.addEdge(this_node);
				});				
				return PTuple2.of(map, this_node);
			});
		};

		final fj.data.List<State<Map<String, Node>, Node>> actions = fj.data.Java8.JavaStream_List(Stream.concat(
				inputs.stream().map(s -> addInput.apply(s))
				,
				enrichment_elements.stream().map(e -> addEnrichment.apply(e))
				));
				
		return State
			.sequence(actions) 
				// ^ (this is a utility to replace the scala "for (a <- x, b <- y, etc)" construct - it calls actions.foldLeft(acc, state -> acc.flatMap(list -> state.map(x -> list.add(x))
			.run(new HashMap<>()) // (starting state)
			._2()
			.toJavaList()
		;
		
		// (NOTE: this is equivalent to:)
		// ie applying the "event" State via the flatMap modifies the state, whereas the "event.map" modifies the result (by combining with the output from the current_state)
		// (so if you had a giant stream with relatively few outputs, eg as determined by an optional, then this would allow you to minimize memory)
//		actions.toJavaList().stream().reduce(
//				State.<Map<String, Node>, List<Node>>constant((List<Node>) new LinkedList<Node>()), //(starting _output_)
//				(curr_state, event) -> curr_state.flatMap(curr_state_result -> event.map(event_result -> {
//					curr_state_result.add(event_result);
//					return curr_state_result;
//				}))
//				,
//				(acc1, acc2) -> acc1
//				)
//				.run(new HashMap<>())  //(starting _state_)
//				//(etc)
//				;
		
	}
	
	/////////////////////

	// GRAPH UTILS (as above, this is a copy paste)
	
	/** Generates ordered list of "identifying strings" based on their underlying dependencies
	 * @param mutable_in
	 * @return
	 */
	protected static Validation<String, List<Node>> generateOrder(final List<Node> mutable_in) {
		//L <- Empty list that will contain the sorted elements
		ArrayList<Node> L = new ArrayList<Node>();

		//S <- Set of all nodes with no incoming edges
		HashSet<Node> S = new HashSet<Node>(); 
		for(Node n : mutable_in){
			n.numOutEdges = n.outEdges.size(); // (store this so can use it later...)
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
		Node cycle_node = null;
		for(Node n : mutable_in){
			if(!n.inEdges.isEmpty()){
				cycle_node = n;
				break;
			}
		}
		return (null != cycle_node)
				? Validation.fail("Cycle present: " + cycle_node.name + ": " + cycle_node.inEdges)
				: Validation.success(L)
				;
	}

	protected static class Node{
		public final String name; //(invariant)
		public int numOutEdges; //(invariant once set)
		public final HashSet<Edge> inEdges; //(mutable)
		public final HashSet<Edge> outEdges; //(mutable)
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
		public String toString() {
			return from.toString() + "->" + to.toString();
		}
		@Override
		public boolean equals(Object obj) {
			Edge e = (Edge)obj;
			return e.from == from && e.to == to;
		}
	}

	/////////////////////
	
	// LOWER LEVEL UTILS
	
	/** Scala<->fj bridge for tuples
	 * @author Alex
	 *
	 * @param <A>
	 * @param <B>
	 */
	public static class PTuple2<A, B> extends P2<A, B> {
		public static <A, B> PTuple2<A, B> of(A a, B b)  { return new PTuple2<A, B>(Tuples._2T(a,  b)); }
		public Tuple2<A, B> t2() { return _t2; }
		protected Tuple2<A, B> _t2;
		protected PTuple2(Tuple2<A, B> t2) {
			_t2 = t2;
		}
		@Override
		public A _1() {
			return _t2._1();
		}
		@Override
		public B _2() {
			return _t2._2();
		}		
	}
	
}
