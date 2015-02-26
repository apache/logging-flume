/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flume.interceptor.SplitInterceptor.Constants.*;

/**
 * Interceptor class that appends a dynamic header to all events.
 * 
 * Properties:
 * <p>
 * 
 * field: the name to use in header insertion. (default is "field")
 * <p>
 * 
 * value: value for default header insertion. (default is "default")
 * <p>
 * 
 * delimiter: delimiter to split event body data. (default is ",")
 * <p>
 * 
 * index: index in splits for value. (default is "0")
 * <p>
 * 
 * 
 * preserveExisting: Whether to preserve an existing value for 'field' (default
 * is true)
 * <p>
 * 
 * Sample config:
 * <p>
 * 
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = <p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = split_interceptor<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = false<p>
 *   agent.sources.r1.interceptors.i1.field = field<p>
 *   agent.sources.r1.interceptors.i1.value = default<p>
 *   agent.sources.r1.interceptors.i1.delimiter = ,<p>
 *   agent.sources.r1.interceptors.i1.index = 0<p>
 * </code>
 * 
 */
public class SplitInterceptor implements Interceptor {

	private static final Logger logger = LoggerFactory.getLogger(SplitInterceptor.class);

	private final boolean preserveExisting;
	private final String field;
	private final String value;
	private final String delimiter;
	private final String index;

	/**
	 * Only {@link SplitInterceptor.Builder} can build me
	 */
	private SplitInterceptor(boolean preserveExisting, String field, String value, String delimiter, String index) {

		this.preserveExisting = preserveExisting;
		this.field = field;
		this.value = value;
		this.delimiter = delimiter;
		this.index = index;
	}

	@Override
	public void initialize() {
		// no-op
	}

	/**
	 * Modifies events in-place.
	 */
	@Override
	public Event intercept(Event event) {

		Map<String, String> headers = event.getHeaders();

		if (preserveExisting && headers.containsKey(field)) {
			return event;
		}

		String[] splits = new String(event.getBody()).split(delimiter);
		if (splits.length > Integer.valueOf(index)) {
			headers.put(field, splits[Integer.valueOf(index)]);
		} else {
			headers.put(field, value);
		}
		return event;
	}

	/**
	 * Delegates to {@link #intercept(Event)} in a loop.
	 * 
	 * @param events
	 * @return
	 */
	@Override
	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}

	@Override
	public void close() {
		// no-op
	}

	/**
	 * Builder which builds new instance of the SplitInterceptor.
	 */
	public static class Builder implements Interceptor.Builder {

		private boolean preserveExisting;
		private String field;
		private String value;
		private String delimiter;
		private String index;

		@Override
		public void configure(Context context) {

			field = context.getString(FIELD, FIELD_DEFAULT);
			value = context.getString(VALUE, VALUE_DEFAULT);
			delimiter = context.getString(DELIMITER, DELIMITER_DEFAULT);
			index = context.getString(INDEX, INDEX_DEFAULT);
			preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DEFAULT);
		}

		@Override
		public Interceptor build() {
			logger.info(String.format("Creating SplitInterceptor: preserveExisting=%s,field=%s,value=%s,delimiter=%s,index=%s", preserveExisting, field, value, delimiter, index));
			return new SplitInterceptor(preserveExisting, field, value, delimiter, index);
		}

	}

	public static class Constants {

		public static final String FIELD = "field";
		public static final String FIELD_DEFAULT = "field";

		public static final String VALUE = "value";
		public static final String VALUE_DEFAULT = "default";

		public static final String DELIMITER = "delimiter";
		public static final String DELIMITER_DEFAULT = ",";

		public static final String INDEX = "index";
		public static final String INDEX_DEFAULT = "0";

		public static final String PRESERVE = "preserveExisting";
		public static final boolean PRESERVE_DEFAULT = true;
	}
}
