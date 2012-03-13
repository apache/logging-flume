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

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
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
  private RpcClient rpcClient = null;


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
    if(rpcClient == null){
      throw new FlumeException("Cannot Append to Appender!" +
          "Appender either closed or not setup correctly!");
    }

    if(!rpcClient.isActive()){
      reconnect();
    }

    //Client created first time append is called.
    Map<String, String> hdrs = new HashMap<String, String>();
    hdrs.put(Log4jAvroHeaders.LOGGER_NAME.toString(), event.getLoggerName());
    hdrs.put(Log4jAvroHeaders.TIMESTAMP.toString(),
        String.valueOf(event.getTimeStamp()));

    //To get the level back simply use
    //LoggerEvent.toLevel(hdrs.get(Integer.parseInt(
    //Log4jAvroHeaders.LOG_LEVEL.toString()))
    hdrs.put(Log4jAvroHeaders.LOG_LEVEL.toString(),
        String.valueOf(event.getLevel().toInt()));
    hdrs.put(Log4jAvroHeaders.MESSAGE_ENCODING.toString(), "UTF8");

    Event flumeEvent = EventBuilder.withBody(event.getMessage().toString(),
        Charset.forName("UTF8"), hdrs);

    try {
      rpcClient.append(flumeEvent);
    } catch (EventDeliveryException e) {
      String msg = "Flume append() failed.";
      LogLog.error(msg);
      throw new FlumeException(msg + " Exception follows.", e);
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
  public synchronized void close() throws FlumeException{
    //Any append calls after this will result in an Exception.
    if (rpcClient != null) {
      rpcClient.close();
      rpcClient = null;
    }
  }

  @Override
  public boolean requiresLayout() {
    return false;
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

  /**
   * Activate the options set using <tt>setPort()</tt>
   * and <tt>setHostname()</tt>
   * @throws FlumeException if the <tt>hostname</tt> and
   *  <tt>port</tt> combination is invalid.
   */
  @Override
  public void activateOptions() throws FlumeException{
    try {
      rpcClient = RpcClientFactory.getInstance(hostname, port);
    } catch (FlumeException e) {
      String errormsg = "RPC client creation failed! " +
          e.getMessage();
      LogLog.error(errormsg);
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

