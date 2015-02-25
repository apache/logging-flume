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

package org.apache.flume.sink.kite.policy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.Syncable;
import org.kitesdk.data.View;

import static org.apache.flume.sink.kite.DatasetSinkConstants.*;

/**
 * A failure policy that writes the raw Flume event to a Kite dataset.
 */
public class SavePolicy implements FailurePolicy {

  private final View<AvroFlumeEvent> dataset;
  private DatasetWriter<AvroFlumeEvent> writer;
  private int nEventsHandled;

  private SavePolicy(Context context) {
    String uri = context.getString(CONFIG_KITE_ERROR_DATASET_URI);
    Preconditions.checkArgument(uri != null, "Must set "
        + CONFIG_KITE_ERROR_DATASET_URI + " when " + CONFIG_FAILURE_POLICY
        + "=save");
    if (Datasets.exists(uri)) {
      dataset = Datasets.load(uri, AvroFlumeEvent.class);
    } else {
      DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
          .schema(AvroFlumeEvent.class)
          .build();
      dataset = Datasets.create(uri, descriptor, AvroFlumeEvent.class);
    }

    nEventsHandled = 0;
  }

  @Override
  public void handle(Event event, Throwable cause) throws EventDeliveryException {
    try {
      if (writer == null) {
        writer = dataset.newWriter();
      }

      final AvroFlumeEvent avroEvent = new AvroFlumeEvent();
      avroEvent.setBody(ByteBuffer.wrap(event.getBody()));
      avroEvent.setHeaders(toCharSeqMap(event.getHeaders()));

      writer.write(avroEvent);
      nEventsHandled++;
    } catch (RuntimeException ex) {
      throw new EventDeliveryException(ex);
    }
  }

  @Override
  public void sync() throws EventDeliveryException {
    if (nEventsHandled > 0) {
      if (Formats.PARQUET.equals(
          dataset.getDataset().getDescriptor().getFormat())) {
        // We need to close the writer on sync if we're writing to a Parquet
        // dataset
        close();
      } else {
        if (writer instanceof Syncable) {
          ((Syncable) writer).sync();
        }
      }
    }
  }

  @Override
  public void close() throws EventDeliveryException {
    if (nEventsHandled > 0) {
      try {
        writer.close();
      } catch (RuntimeException ex) {
        throw new EventDeliveryException(ex);
      } finally {
        writer = null;
        nEventsHandled = 0;
      }
    }
  }

  /**
   * Helper function to convert a map of String to a map of CharSequence.
   */
  private static Map<CharSequence, CharSequence> toCharSeqMap(
      Map<String, String> map) {
    return Maps.<CharSequence, CharSequence>newHashMap(map);
  }

  public static class Builder implements FailurePolicy.Builder {

    @Override
    public FailurePolicy build(Context config) {
      return new SavePolicy(config);
    }

  }
}
