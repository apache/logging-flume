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

import java.io.File;
import java.util.Map.Entry;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Compiler;
import com.cloudera.cdk.morphline.base.FaultTolerance;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Notifications;
import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * A {@link MorphlineHandler} that processes it's events using a morphline {@link Command} chain.
 */
public class MorphlineHandlerImpl implements MorphlineHandler {

  private MorphlineContext morphlineContext;
  private Command morphline;
  private Command finalChild;
  private String morphlineFileAndId;
  
  public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
  public static final String MORPHLINE_ID_PARAM = "morphlineId";
  
  /**
   * Morphline variables can be passed from flume.conf to the morphline, e.g.:
   * agent.sinks.solrSink.morphlineVariable.zkHost=127.0.0.1:2181/solr
   */
  public static final String MORPHLINE_VARIABLE_PARAM = "morphlineVariable";

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineHandlerImpl.class);
  
  // For test injection
  void setMorphlineContext(MorphlineContext morphlineContext) {
    this.morphlineContext = morphlineContext;
  }

  // for interceptor
  void setFinalChild(Command finalChild) {
    this.finalChild = finalChild;
  }

  @Override
  public void configure(Context context) {
    if (morphlineContext == null) {
      FaultTolerance faultTolerance = new FaultTolerance(
          context.getBoolean(FaultTolerance.IS_PRODUCTION_MODE, false), 
          context.getBoolean(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false),
          context.getString(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES));
      
      morphlineContext = new MorphlineContext.Builder()
        .setExceptionHandler(faultTolerance)
        .setMetricRegistry(new MetricRegistry())
        .build();
    }
    
    String morphlineFile = context.getString(MORPHLINE_FILE_PARAM);
    String morphlineId = context.getString(MORPHLINE_ID_PARAM);
    if (morphlineFile == null || morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing parameter: " + MORPHLINE_FILE_PARAM, null);
    }
    Config override = ConfigFactory.parseMap(context.getSubProperties(MORPHLINE_VARIABLE_PARAM + "."));
    morphline = new Compiler().compile(new File(morphlineFile), morphlineId, morphlineContext, finalChild, override);      
    morphlineFileAndId = morphlineFile + "@" + morphlineId;
  }

  @Override
  public void process(Event event) {
    Record record = new Record();
    for (Entry<String, String> entry : event.getHeaders().entrySet()) {
      record.put(entry.getKey(), entry.getValue());
    }
    byte[] bytes = event.getBody();
    if (bytes != null && bytes.length > 0) {
      record.put(Fields.ATTACHMENT_BODY, bytes);
    }    
    try {
      Notifications.notifyStartSession(morphline);
      if (!morphline.process(record)) {
        LOG.warn("Morphline {} failed to process record: {}", morphlineFileAndId, record);
      }
    } catch (RuntimeException t) {
      morphlineContext.getExceptionHandler().handleException(t, record);
    }
  }

  @Override
  public void beginTransaction() {
    Notifications.notifyBeginTransaction(morphline);      
  }

  @Override
  public void commitTransaction() {
    Notifications.notifyCommitTransaction(morphline);      
  }

  @Override
  public void rollbackTransaction() {
    Notifications.notifyRollbackTransaction(morphline);            
  }

  @Override
  public void stop() {
    Notifications.notifyShutdown(morphline);
  }

}
