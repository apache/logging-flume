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
package org.apache.flume.clients.log4jappender;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 *
 * Appends Log4j Events to an external Flume client which is decribed by
 * the Log4j configuration file. The appender takes two required parameters:
 *<p>
 *<strong>Hostname</strong> : This is the hostname of the first hop
 *at which Flume (through an AvroSource) is listening for events.
 *</p>
 *<p>
 *<strong>Port</strong> : This the port on the above host where the Flume
 *Source is listening for events.
 *</p>
 *A sample log4j properties file which appends to a source would look like:
 *<pre><p>
 *log4j.appender.out2 = org.apache.flume.clients.log4jappender.Log4jAppender
 *log4j.appender.out2.Port = 25430
 *log4j.appender.out2.Hostname = foobarflumesource.com
 *log4j.logger.org.apache.flume.clients.log4jappender = DEBUG,out2</p></pre>
 *<p><i>Note: Change the last line to the package of the class(es), that will
 *do the appending.For example if classes from the package
 *com.bar.foo are appending, the last line would be:</i></p>
 *<pre><p>log4j.logger.com.bar.foo = DEBUG,out2</p></pre>
 *
 *
 */
public class Log4jAppender extends AppenderSkeleton {

  private String hostname;
  private int port;
  private boolean unsafeMode = false;
  private long timeout = RpcClientConfigurationConstants
    .DEFAULT_REQUEST_TIMEOUT_MILLIS;
  private boolean avroReflectionEnabled;
  private String avroSchemaUrl;

  RpcClient rpcClient = null;


  /**
   * If this constructor is used programmatically rather than from a log4j conf
   * you must set the <tt>port</tt> and <tt>hostname</tt> and then call
   * <tt>activateOptions()</tt> before calling <tt>append()</tt>.
   */
  public Log4jAppender(){
  }

  /**
   * Sets the hostname and port. Even if these are passed the
   * <tt>activateOptions()</tt> function must be called before calling
   * <tt>append()</tt>, else <tt>append()</tt> will throw an Exception.
   * @param hostname The first hop where the client should connect to.
   * @param port The port to connect on the host.
   *
   */
  public Log4jAppender(String hostname, int port){
    this.hostname = hostname;
    this.port = port;
  }

  /**
   * Append the LoggingEvent, to send to the first Flume hop.
   * @param event The LoggingEvent to be appended to the flume.
   * @throws FlumeException if the appender was closed,
   * or the hostname and port were not setup, there was a timeout, or there
   * was a connection error.
   */
  @Override
  public synchronized void append(LoggingEvent event) throws FlumeException{
    //If rpcClient is null, it means either this appender object was never
    //setup by setting hostname and port and then calling activateOptions
    //or this appender object was closed by calling close(), so we throw an
    //exception to show the appender is no longer accessible.
    if (rpcClient == null) {
      String errorMsg = "Cannot Append to Appender! Appender either closed or" +
        " not setup correctly!";
      LogLog.error(errorMsg);
      if (unsafeMode) {
        return;
      }
      throw new FlumeException(errorMsg);
    }

    if(!rpcClient.isActive()){
      reconnect();
    }

    //Client created first time append is called.
    Map<String, String> hdrs = new HashMap<String, String>();
    hdrs.put(Log4jAvroHeaders.LOGGER_NAME.toString(), event.getLoggerName());
    hdrs.put(Log4jAvroHeaders.TIMESTAMP.toString(),
        String.valueOf(event.timeStamp));

    //To get the level back simply use
    //LoggerEvent.toLevel(hdrs.get(Integer.parseInt(
    //Log4jAvroHeaders.LOG_LEVEL.toString()))
    hdrs.put(Log4jAvroHeaders.LOG_LEVEL.toString(),
        String.valueOf(event.getLevel().toInt()));

    Event flumeEvent;
    Object message = event.getMessage();
    if (message instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) message;
      populateAvroHeaders(hdrs, record.getSchema(), message);
      flumeEvent = EventBuilder.withBody(serialize(record, record.getSchema()), hdrs);
    } else if (message instanceof SpecificRecord || avroReflectionEnabled) {
      Schema schema = ReflectData.get().getSchema(message.getClass());
      populateAvroHeaders(hdrs, schema, message);
      flumeEvent = EventBuilder.withBody(serialize(message, schema), hdrs);
    } else {
      hdrs.put(Log4jAvroHeaders.MESSAGE_ENCODING.toString(), "UTF8");
      String msg = layout != null ? layout.format(event) : message.toString();
      flumeEvent = EventBuilder.withBody(msg, Charset.forName("UTF8"), hdrs);
    }

    try {
      rpcClient.append(flumeEvent);
    } catch (EventDeliveryException e) {
      String msg = "Flume append() failed.";
      LogLog.error(msg);
      if (unsafeMode) {
        return;
      }
      throw new FlumeException(msg + " Exception follows.", e);
    }
  }

  private Schema schema;
  private ByteArrayOutputStream out;
  private DatumWriter<Object> writer;
  private BinaryEncoder encoder;

  protected void populateAvroHeaders(Map<String, String> hdrs, Schema schema,
      Object message) {
    if (avroSchemaUrl != null) {
      hdrs.put(Log4jAvroHeaders.AVRO_SCHEMA_URL.toString(), avroSchemaUrl);
      return;
    }
    LogLog.warn("Cannot find ID for schema. Adding header for schema, " +
        "which may be inefficient. Consider setting up an Avro Schema Cache.");
    hdrs.put(Log4jAvroHeaders.AVRO_SCHEMA_LITERAL.toString(), schema.toString());
  }

  private byte[] serialize(Object datum, Schema datumSchema) throws FlumeException {
    if (schema == null || !datumSchema.equals(schema)) {
      schema = datumSchema;
      out = new ByteArrayOutputStream();
      writer = new ReflectDatumWriter<Object>(schema);
      encoder = EncoderFactory.get().binaryEncoder(out, null);
    }
    out.reset();
    try {
      writer.write(datum, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (IOException e) {
      throw new FlumeException(e);
    }
  }

  //This function should be synchronized to make sure one thread
  //does not close an appender another thread is using, and hence risking
  //a null pointer exception.
  /**
   * Closes underlying client.
   * If <tt>append()</tt> is called after this function is called,
   * it will throw an exception.
   * @throws FlumeException if errors occur during close
   */
  @Override
  public synchronized void close() throws FlumeException {
    // Any append calls after this will result in an Exception.
    if (rpcClient != null) {
      try {
        rpcClient.close();
      } catch (FlumeException ex) {
        LogLog.error("Error while trying to close RpcClient.", ex);
        if (unsafeMode) {
          return;
        }
        throw ex;
      } finally {
        rpcClient = null;
      }
    } else {
      String errorMsg = "Flume log4jappender already closed!";
      LogLog.error(errorMsg);
      if(unsafeMode) {
        return;
      }
      throw new FlumeException(errorMsg);
    }
  }

  @Override
  public boolean requiresLayout() {
    // This method is named quite incorrectly in the interface. It should
    // probably be called canUseLayout or something. According to the docs,
    // even if the appender can work without a layout, if it can work with one,
    // this method must return true.
    return true;
  }

  /**
   * Set the first flume hop hostname.
   * @param hostname The first hop where the client should connect to.
   */
  public void setHostname(String hostname){
    this.hostname = hostname;
  }

  /**
   * Set the port on the hostname to connect to.
   * @param port The port to connect on the host.
   */
  public void setPort(int port){
    this.port = port;
  }

  public void setUnsafeMode(boolean unsafeMode) {
    this.unsafeMode = unsafeMode;
  }

  public boolean getUnsafeMode() {
    return unsafeMode;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public long getTimeout() {
    return this.timeout;
  }

  public void setAvroReflectionEnabled(boolean avroReflectionEnabled) {
    this.avroReflectionEnabled = avroReflectionEnabled;
  }

  public void setAvroSchemaUrl(String avroSchemaUrl) {
    this.avroSchemaUrl = avroSchemaUrl;
  }

  /**
   * Activate the options set using <tt>setPort()</tt>
   * and <tt>setHostname()</tt>
   *
   * @throws FlumeException if the <tt>hostname</tt> and
   *                        <tt>port</tt> combination is invalid.
   */
  @Override
  public void activateOptions() throws FlumeException {
    Properties props = new Properties();
    props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
    props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1",
      hostname + ":" + port);
    props.setProperty(RpcClientConfigurationConstants.CONFIG_CONNECT_TIMEOUT,
     String.valueOf(timeout));
    props.setProperty(RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT,
      String.valueOf(timeout));
    try {
      rpcClient = RpcClientFactory.getInstance(props);
      if (layout != null) {
        layout.activateOptions();
      }
    } catch (FlumeException e) {
      String errormsg = "RPC client creation failed! " +
        e.getMessage();
      LogLog.error(errormsg);
      if (unsafeMode) {
        return;
      }
      throw e;
    }
  }

  /**
   * Make it easy to reconnect on failure
   * @throws FlumeException
   */
  private void reconnect() throws FlumeException {
    close();
    activateOptions();
  }
}

