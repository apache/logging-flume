/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.core;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringEscapeUtils;

import com.google.common.base.Preconditions;

/**
 * This provides a single implementation of a fields map that can be used by
 * subclasses and adaptors
 * 
 * TODO (jon) Consider changing EventImpl to put all fields into Map (depends on
 * cost)
 */
abstract public class EventBaseImpl extends Event {
  protected Map<String, byte[]> fields;

  protected EventBaseImpl() {
    this.fields = new HashMap<String, byte[]>();
  }

  /**
   * Ownership of this fields argument is assumed to be transferred to this
   * object, and it is assumed that a reference to fields will not modified
   * elsewhere
   */
  protected EventBaseImpl(Map<String, byte[]> fields) {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  @Override
  public byte[] get(String attr) {
    return fields.get(attr);
  }

  @Override
  public void set(String attr, byte[] v) {
    if (fields.get(attr) != null) {
      throw new IllegalArgumentException(
          "Event already had an event with attribute " + attr);
    }
    fields.put(attr, v);
  }

  /**
   * Instead of package private, I make this method return an unmodifiable map.
   * I don't want external methods to modify the internal map
   */
  public Map<String, byte[]> getAttrs() {
    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String toString() {
    String mbody = StringEscapeUtils.escapeJava(new String(getBody()));
    return getHost() + " [" + getPriority().toString() + " "
        + new Date(getTimestamp()) + "] " + mbody;
  }
  
  @Override
  public void hierarchicalMerge(String prefix, Event e) {
    Preconditions.checkNotNull(e, "hierarchicalMerge called with null event");
    for (Entry<String, byte[]> field : e.getAttrs().entrySet()) {
      String key = prefix + "." + field.getKey();
      if (get(key) != null) {
        // this report currently doesn't have the attribute
        continue;
      }
      byte[] val = e.get(key);
      if (val != null) {
        set(key, val);
      }
    }
  }    

  @Override
  public void merge(Event e) {
    Preconditions.checkNotNull(e, "merge called with null event");
    for (Entry<String, byte[]> field : e.getAttrs().entrySet()) {
      if (get(field.getKey()) != null) {
        // this report currently doesn't have the attribute
        continue;
      }
      byte[] val = e.get(field.getKey());
      if (val != null) {
        set(field.getKey(), val);
      }
    }    
  }

}
