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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

/**
 * Flume Interceptor that executes a morphline on events that are intercepted.
 * 
 * Currently, there is a restriction in that the morphline must not generate more than one output
 * record for each input event.
 */
public class MorphlineInterceptor implements Interceptor {

  private final Context context;
  private final Queue<LocalMorphlineInterceptor> pool = new ConcurrentLinkedQueue<LocalMorphlineInterceptor>();
  
  protected MorphlineInterceptor(Context context) {
    Preconditions.checkNotNull(context);
    this.context = context;
    returnToPool(new LocalMorphlineInterceptor(context)); // fail fast on morphline compilation exception
  }

  @Override
  public void initialize() {
  }

  @Override
  public void close() {
    LocalMorphlineInterceptor interceptor;
    while ((interceptor = pool.poll()) != null) {
      interceptor.close();
    }
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    LocalMorphlineInterceptor interceptor = borrowFromPool();
    List<Event> results = interceptor.intercept(events);
    returnToPool(interceptor);
    return results;
  }
  
  @Override
  public Event intercept(Event event) {
    LocalMorphlineInterceptor interceptor = borrowFromPool();
    Event result = interceptor.intercept(event);
    returnToPool(interceptor);
    return result;
  }

  private void returnToPool(LocalMorphlineInterceptor interceptor) {
    pool.add(interceptor);
  }
  
  private LocalMorphlineInterceptor borrowFromPool() {
    LocalMorphlineInterceptor interceptor = pool.poll();
    if (interceptor == null) {
      interceptor = new LocalMorphlineInterceptor(context);
    }
    return interceptor;
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Builder implementations MUST have a public no-arg constructor */
  public static class Builder implements Interceptor.Builder {

    private Context context;

    public Builder() {
    }

    @Override
    public MorphlineInterceptor build() {
      return new MorphlineInterceptor(context);
    }

    @Override
    public void configure(Context context) {
      this.context = context;
    }

  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class LocalMorphlineInterceptor implements Interceptor {

    private final MorphlineHandlerImpl morphline;
    private final Collector collector;
    
    protected LocalMorphlineInterceptor(Context context) {
      this.morphline = new MorphlineHandlerImpl();
      this.collector = new Collector();
      this.morphline.setFinalChild(collector);
      this.morphline.configure(context);
    }

    @Override
    public void initialize() {
    }

    @Override
    public void close() {
      morphline.stop();
    }

    @Override
    public List<Event> intercept(List<Event> events) {
      List results = new ArrayList(events.size());
      for (Event event : events) {
        event = intercept(event);
        if (event != null) {
          results.add(event);
        }
      }
      return results;
    }

    @Override
    public Event intercept(Event event) {
      collector.reset();
      morphline.process(event);
      List<Record> results = collector.getRecords();
      if (results.size() == 0) {
        return null;
      }
      if (results.size() > 1) {
        throw new FlumeException(getClass().getName() + 
            " must not generate more than one output record per input event");
      }
      Event result = toEvent(results.get(0));    
      return result;
    }
    
    private Event toEvent(Record record) {
      Map<String, String> headers = new HashMap();
      Map<String, Collection<Object>> recordMap = record.getFields().asMap();
      byte[] body = null;
      for (Map.Entry<String, Collection<Object>> entry : recordMap.entrySet()) {
        if (entry.getValue().size() > 1) {
          throw new FlumeException(getClass().getName()
              + " must not generate more than one output value per record field");
        }
        assert entry.getValue().size() != 0; // guava guarantees that
        Object firstValue = entry.getValue().iterator().next();
        if (Fields.ATTACHMENT_BODY.equals(entry.getKey())) {
          if (firstValue instanceof byte[]) {
            body = (byte[]) firstValue;
          } else if (firstValue instanceof InputStream) {
            try {
              body = ByteStreams.toByteArray((InputStream) firstValue);
            } catch (IOException e) {
              throw new FlumeException(e);
            }            
          } else {
            throw new FlumeException(getClass().getName()
                + " must non generate attachments that are not a byte[] or InputStream");
          }
        } else {
          headers.put(entry.getKey(), firstValue.toString());
        }
      }
      return EventBuilder.withBody(body, headers);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Collector implements Command {
    
    private final List<Record> results = new ArrayList();
    
    public List<Record> getRecords() {
      return results;
    }
    
    public void reset() {
      results.clear();
    }

    @Override
    public Command getParent() {
      return null;
    }
    
    @Override
    public void notify(Record notification) {
    }

    @Override
    public boolean process(Record record) {
      Preconditions.checkNotNull(record);
      results.add(record);
      return true;
    }
    
  }

}
