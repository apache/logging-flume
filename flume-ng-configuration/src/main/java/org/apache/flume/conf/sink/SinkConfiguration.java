/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.conf.sink;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.conf.FlumeConfigurationError;
import org.apache.flume.conf.FlumeConfigurationErrorType;
import org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning;

public class SinkConfiguration extends ComponentConfiguration {

  protected String channel;

  public SinkConfiguration(String componentName) {
    super(componentName);
  }

  public String getChannel() {
    return channel;
  }

  public void getChannel(String channel) {
    this.channel = channel;
  }

  public void configure(Context context) throws ConfigurationException {
    super.configure(context);
    this.channel = context.getString("channel");
    if (this.channel == null || this.channel.isEmpty()) {
      errors
          .add(new FlumeConfigurationError(componentName, "channel",
              FlumeConfigurationErrorType.PROPERTY_VALUE_NULL,
              ErrorOrWarning.ERROR));
      throw new ConfigurationException("No channel configured for sink: "
          + this.getComponentName());
    }
  }

  @Override
  public String toString(int indentCount){
    StringBuilder indentSb = new StringBuilder("");

    for (int i = 0; i < indentCount; i++) {
      indentSb.append(FlumeConfiguration.INDENTSTEP);
    }

    String basicStr = super.toString(indentCount);
    StringBuilder sb = new StringBuilder();
    sb.append(basicStr).append(FlumeConfiguration.INDENTSTEP).append(
        "CHANNEL:").append(this.channel).append(
        FlumeConfiguration.NEWLINE);
    return sb.toString();
  }

  public enum SinkConfigurationType {
    /**
     * Place holder for custom sinks not part of this enumeration.
     */
    OTHER(null),

    /**
     * Null sink
     *
     * @see NullSink
     */
    NULL("org.apache.flume.conf.sink.NullSinkConfiguration"),

    /**
     * Logger sink
     *
     * @see LoggerSink
     */
    LOGGER(null),

    /**
     * Rolling file sink
     *
     * @see RollingFileSink
     */
    FILE_ROLL("org.apache.flume.conf.sink.RollingFileSinkConfiguration"),

    /**
     * HDFS Sink provided by org.apache.flume.sink.hdfs.HDFSEventSink
     */
    HDFS("org.apache.flume.conf.sink.HDFSSinkConfiguration"),

    /**
     * IRC Sink provided by org.apache.flume.sink.irc.IRCSink
     */
    IRC("org.apache.flume.conf.sink.IRCSinkConfiguration"),

    /**
     * Avro sink
     *
     * @see AvroSink
     */
    AVRO("org.apache.flume.conf.sink.AvroSinkConfiguration"),

    /**
     * Thrift sink
     *
     * @see ThriftSink
     */
    THRIFT("org.apache.flume.conf.sink.ThriftSinkConfiguration"),

    /**
     * ElasticSearch Sink
     *
     * @see org.apache.flume.sink.elasticsearch.ElasticSearchSink
     */
    ELASTICSEARCH("org.apache.flume.sink.elasticsearch.ElasticSearchSinkConfiguration"),

    /**
     * HBase Sink
     *
     * @see org.apache.flume.sink.hbase.HBaseSink
     */
    HBASE("org.apache.flume.sink.hbase.HBaseSinkConfiguration"),

    /**
     * AsyncHBase Sink
     *
     * @see org.apache.flume.sink.hbase.AsyncHBaseSink
     */
    ASYNCHBASE("org.apache.flume.sink.hbase.HBaseSinkConfiguration"),


    /**
     * MorphlineSolr sink
     *
     * @see org.apache.flume.sink.solr.morphline.MorphlineSolrSink
     */
    MORPHLINE_SOLR("org.apache.flume.sink.solr.morphline" +
      ".MorphlineSolrSinkConfiguration");

    private final String sinkConfigurationName;

    private SinkConfigurationType(String type) {
      this.sinkConfigurationName = type;
    }

    public String getSinkConfigurationType() {
      return this.sinkConfigurationName;
    }

    @SuppressWarnings("unchecked")
    public SinkConfiguration getConfiguration(String name)
        throws ConfigurationException {
      if (this.equals(SinkConfigurationType.OTHER)) {
        return new SinkConfiguration(name);
      }
      Class<? extends SinkConfiguration> clazz;
      SinkConfiguration instance = null;
      try {
        if (sinkConfigurationName != null) {
          clazz =
              (Class<? extends SinkConfiguration>) Class
                  .forName(sinkConfigurationName);
          instance = clazz.getConstructor(String.class).newInstance(name);
        } else {
          return new SinkConfiguration(name);
        }
      } catch (ClassNotFoundException e) {
        // Could not find the configuration stub, do basic validation
        instance = new SinkConfiguration(name);
        // Let the caller know that this was created because of this exception.
        instance.setNotFoundConfigClass();
      } catch (Exception e){
        throw new ConfigurationException("Couldn't create configuration", e);
      }
      return instance;
    }

  }

}
