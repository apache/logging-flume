/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hbase;

import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class JsonHbaseEventSerializer implements HbaseEventSerializer {

	private static final String CHARSET_CONFIG = "charset";
	private static final String CHARSET_DEFAULT = "UTF-8";

	protected static final AtomicLong nonce = new AtomicLong(0);

	protected byte[] cf;
	private byte[] payload;
	private Map<String, String> headers;
	private Charset charset;

	@Override
	public void configure(Context context) {
		charset = Charset.forName(context.getString(CHARSET_CONFIG, CHARSET_DEFAULT));
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}

	@Override
	public void initialize(Event event, byte[] columnFamily) {
		this.headers = event.getHeaders();
		this.payload = event.getBody();
		this.cf = columnFamily;
	}
	
	protected byte[] getRowKey(Calendar cal) {
		String rowKey = String.format("%s%s", cal.getTimeInMillis(),nonce.getAndIncrement());
		return rowKey.getBytes(charset);
	}

	protected byte[] getRowKey() {
		return getRowKey(Calendar.getInstance());
	}

	@Override
	public List<Row> getActions() throws FlumeException {
		List<Row> actions = Lists.newArrayList();
		try {
		
			byte[] rowKey = getRowKey();
			
			Put put = new Put(rowKey);
			
			Map<String,Object> maps = new ObjectMapper().readValue(new String(payload), Map.class);
			
			maps.remove("headers");
			
			for (Entry<String, Object>entry:maps.entrySet()) {
				put.add(cf, entry.getKey().getBytes(charset), entry.getValue().toString().getBytes(charset));
	        }
	   
	        for (Map.Entry<String, String> entry : headers.entrySet()) {
				put.add(cf, entry.getKey().getBytes(charset), entry.getValue().getBytes(charset));
			}
			
			actions.add(put);
		} catch (Exception e) {
			throw new FlumeException("Could not get row key!", e);
		}
		return actions;
	}

	@Override
	public List<Increment> getIncrements() {
		return Lists.newArrayList();
	}

	@Override
	public void close() {
	}
}
