/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.solr.morphline;

import org.apache.flume.Context;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.base.FaultTolerance;


/**
 * Flume sink that extracts search documents from Flume events, processes them using a morphline
 * {@link Command} chain, and loads them into Apache Solr.
 */
public class MorphlineSolrSink extends MorphlineSink {

  public MorphlineSolrSink() {
    super();
  }
  
  /** For testing only */
  protected MorphlineSolrSink(MorphlineHandler handler) {
    super(handler);
  }

  @Override
  public void configure(Context context) {
    if (context.getString(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES) == null) {
      context.put(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES, 
          "org.apache.solr.client.solrj.SolrServerException");      
    }
    super.configure(context);
  }

}
