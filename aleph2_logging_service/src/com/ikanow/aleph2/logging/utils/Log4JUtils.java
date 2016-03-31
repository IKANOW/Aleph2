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
package com.ikanow.aleph2.logging.utils;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.Level;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * @author Burch
 *
 */
public class Log4JUtils {
	private static SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static String message_format = "%s [%s.%s] %s %s %s"; // <date> [<subsystem>.<command>] <level> <class:line> <message> <other_fields=other_values>
	private static String field_format = " %s=%s";
	public static String getLog4JMessage(final JsonNode logObject, final Level level, final StackTraceElement stack, final String date_field, final Map<String, Object> map, final String hostname) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format(message_format, date_format.format(new Date(logObject.get(date_field).asLong())), logObject.get("subsystem").asText(), logObject.get("command").asText(), level.name(), stack, logObject.get("message").asText()));
		sb.append(String.format(field_format, "bucket", logObject.get("bucket").asText()));
		sb.append(String.format(field_format, "hostname", hostname));
		Optional.ofNullable(map).orElse(Collections.emptyMap()).entrySet().stream().forEach(e -> sb.append(String.format(field_format, e.getKey(), e.getValue())));
		return sb.toString();
	}
}
