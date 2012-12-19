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
package org.apache.flume.serialization;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

/**
 * <p>
 * This interface provides callbacks for important serialization-related events.
 * This allows generic implementations of serializers to be plugged in, allowing
 * implementations of this interface to do arbitrary header and message
 * formatting, as well as file and message framing.
 * </p>
 *
 * <p>
 * The following general semantics should be used by drivers that call this
 * interface:
 *
 * <pre>
 * // open file (for example... or otherwise create some new stream)
 * OutputStream out = new FileOutputStream(file); // open for create
 *
 * // build serializer using builder interface
 * EventSerializer serializer = builder.build(ctx, out);
 *
 * // hook to write header (since in this case we opened the file for create)
 * serializer.afterCreate();
 *
 * // write one or more events
 * serializer.write(event1);
 * serializer.write(event2);
 * serializer.write(event3);
 *
 * // periodically flush any internal buffers from EventSerializer.write()
 * serializer.flush();
 *
 * // The driver responsible for specifying and implementing its durability
 * // semantics (if any) for flushing or syncing the underlying stream.
 * out.flush();
 *
 * // when closing the file...
 *
 * // make sure we got all buffered events flushed from the serializer
 * serializer.flush();
 *
 * // write trailer before closing file
 * serializer.beforeClose();
 *
 * // Driver is responsible for flushing the underlying stream, if needed,
 * // before closing it.
 * out.flush();
 * out.close();
 * </pre>
 *
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface EventSerializer {

  /**
   * {@link Context} prefix
   */
  public static String CTX_PREFIX = "serializer.";

  /**
   * Hook to write a header after file is opened for the first time.
   */
  public void afterCreate() throws IOException;

  /**
   * Hook to handle any framing needed when file is re-opened (for write).<br/>
   * Could have been named {@code afterOpenForAppend()}.
   */
  public void afterReopen() throws IOException;

  /**
   * Serialize and write the given event.
   * @param event Event to write to the underlying stream.
   * @throws IOException
   */
  public void write(Event event) throws IOException;

  /**
   * Hook to flush any internal write buffers to the underlying stream.
   * It is NOT necessary for an implementation to then call flush() / sync()
   * on the underlying stream itself, since those semantics would be provided
   * by the driver that calls this API.
   */
  public void flush() throws IOException;

  /**
   * Hook to write a trailer before the stream is closed.
   * Implementations must not buffer data in this call since
   * EventSerializer.flush() is not guaranteed to be called after beforeClose().
   */
  public void beforeClose() throws IOException;

  /**
   * Specify whether this output format supports reopening files for append.
   * For example, this method should return {@code false} if
   * {@link beforeClose()} writes a trailer that "finalizes" the file
   * (this type of behavior is file format-specific).<br/>
   * Could have been named {@code supportsAppend()}.
   */
  public boolean supportsReopen();

  /**
   * Knows how to construct this event serializer.<br/>
   * <b>Note: Implementations MUST provide a public a no-arg constructor.</b>
   */
  public interface Builder {
    public EventSerializer build(Context context, OutputStream out);
  }

}
