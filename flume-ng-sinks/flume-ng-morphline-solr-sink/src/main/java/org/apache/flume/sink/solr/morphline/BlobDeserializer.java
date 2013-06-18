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
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * A deserializer that reads a Binary Large Object (BLOB) per event, typically
 * one BLOB per file; To be used in conjunction with Flume SpoolDirectorySource.
 * <p>
 * Note that this approach is not suitable for very large objects because it
 * buffers up the entire BLOB.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlobDeserializer implements EventDeserializer {

  private ResettableInputStream in;
  private final int maxBlobLength;
  private volatile boolean isOpen;

  public static final String MAX_BLOB_LENGTH_KEY = "maxBlobLength";
  public static final int MAX_BLOB_LENGTH_DEFAULT = 100 * 1000 * 1000;

  private static final int DEFAULT_BUFFER_SIZE = 1024 * 8;
  private static final Logger LOGGER = LoggerFactory.getLogger(BlobDeserializer.class);
      
  protected BlobDeserializer(Context context, ResettableInputStream in) {
    this.in = in;
    this.maxBlobLength = context.getInteger(MAX_BLOB_LENGTH_KEY, MAX_BLOB_LENGTH_DEFAULT);
    if (this.maxBlobLength <= 0) {
      throw new ConfigurationException("Configuration parameter " + MAX_BLOB_LENGTH_KEY
          + " must be greater than zero: " + maxBlobLength);
    }
    this.isOpen = true;
  }

  /**
   * Reads a BLOB from a file and returns an event
   * @return Event containing a BLOB
   * @throws IOException
   */
  @SuppressWarnings("resource")
  @Override
  public Event readEvent() throws IOException {
    ensureOpen();
    ByteArrayOutputStream blob = null;
    byte[] buf = new byte[Math.min(maxBlobLength, DEFAULT_BUFFER_SIZE)];
    int blobLength = 0;
    int n = 0;
    while ((n = in.read(buf, 0, Math.min(buf.length, maxBlobLength - blobLength))) != -1) {
      if (blob == null) {
        blob = new ByteArrayOutputStream(n);
      }
      blob.write(buf, 0, n);
      blobLength += n;
      if (blobLength >= maxBlobLength) {
        LOGGER.warn("File length exceeds maxBlobLength ({}), truncating BLOB event!", maxBlobLength);
        break;
      }
    }
    
    if (blob == null) {
      return null;
    } else {
      return EventBuilder.withBody(blob.toByteArray());
    }
  }
  
  /**
   * Batch BLOB read
   * @param numEvents Maximum number of events to return.
   * @return List of events containing read BLOBs
   * @throws IOException
   */
  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    ensureOpen();
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      } else {
        break;
      }
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    ensureOpen();
    in.mark();
  }

  @Override
  public void reset() throws IOException {
    ensureOpen();
    in.reset();
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      reset();
      in.close();
      isOpen = false;
    }
  }

  private void ensureOpen() {
    if (!isOpen) {
      throw new IllegalStateException("Serializer has been closed");
    }
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Builder implementations MUST have a public no-arg constructor */
  public static class Builder implements EventDeserializer.Builder {

    @Override
    public BlobDeserializer build(Context context, ResettableInputStream in) {      
      return new BlobDeserializer(context, in);
    }

  }

}
