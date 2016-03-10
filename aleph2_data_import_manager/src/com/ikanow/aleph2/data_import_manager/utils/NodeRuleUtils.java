/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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

package com.ikanow.aleph2.data_import_manager.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ikanow.aleph2.data_import_manager.data_model.DataImportConfigurationBean;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;

/** Utilities for checking node rules
 * @author Alex
 */
public class NodeRuleUtils {

	/**
	 * Checks to see if the current node matches the requested node rules
	 * @param bucket 
	 * 
	 * @return
	 */
	public static boolean canRunOnThisNode(
			final Stream<Optional<Collection<String>>> stream_of_rules, 
			final DataImportActorContext context) {
		
		return stream_of_rules.allMatch(maybe_rules -> {
		
			final DataImportConfigurationBean config = context.getDataImportConfigurationBean();		
			final Set<String> bucket_rules = maybe_rules.map(rules -> rules.stream().collect(Collectors.toSet())).orElse(Collections.emptySet()) ;
	
			//if we don't have any rules to follow, just allow it to run
			if ( bucket_rules.isEmpty() )
				return true;
			
			//check if the bucket rules match the config rules
			final String hostname = context.getInformationService().getHostname(); //this is my hostname for comparing globs/regex to
			//loop iteratively so we can kick out early if we find a match
			for ( final String rule : bucket_rules ) {
				if ( testNodeRule(rule, hostname, config.node_rules()) ) {
					return true;
				}
			}
			
			//we fell the whole way through, not a single rule was passed therefore
			return false;
		});
	}

	/**
	 * Tests a node rule to see if it passes
	 * 1. EXCLUSIVE (starts with -) or INCLUSIVE (starts with + or something else)
	 * 2. glob or regex (/pattern/flags) for hostnames OR $(glob or regex) for rule
	 * 3. We pass if ANY rule is accepted
	 * 
	 * @param rule
	 * @param hostname
	 * @return
	 */
	private static boolean testNodeRule(final String rule, final String hostname, final Set<String> node_rules) {
		final boolean exclusive = rule.startsWith("-");		
		final String rule_wo_exclusive = rule.substring((rule.startsWith("-") || rule.startsWith("+")) ? 1 : 0); 
		final boolean is_node_rule = rule_wo_exclusive.startsWith("$");
		final String rule_wo_exclusive_hostname = rule_wo_exclusive.substring(rule_wo_exclusive.startsWith("$") ? 1 : 0);
		final Pattern pattern = PatternUtils.createPatternFromRegexOrGlob(rule_wo_exclusive_hostname);
		
		if ( is_node_rule ) {	
			//is node rule, check against all known node rules for a match		
			for ( String n_r : node_rules ) {
				//check if matches rule and we want to match on this (or opposite)
				if (pattern.matcher(n_r).find() == !exclusive)
					return true;
			}				
		} else {
			//is hostname rule, check if matches hostname and we want to match on this (or opposite)
			if (pattern.matcher(hostname).find() == !exclusive)
				return true;			
		}
		return false;
	}

}
