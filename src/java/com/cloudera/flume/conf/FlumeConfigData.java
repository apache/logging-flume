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
package com.cloudera.flume.conf;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * Stores configuration data for a Flume agent. Since we use multiple RPC
 * frameworks with slightly different data formats, this class provides
 * translation to and from those transport-specific types.
 */
@XmlRootElement
public class FlumeConfigData {
  @XmlTransient
  public long timestamp;
  @XmlTransient
  public String sourceConfig;
  @XmlTransient
  public String sinkConfig;
  @XmlTransient
  public long sourceVersion;
  @XmlTransient
  public long sinkVersion;
  @XmlTransient
  public String flowID;

  public FlumeConfigData(long timestamp, String sourceConfig,
      String sinkConfig, long sourceVersion, long sinkVersion, String flowID) {
    this.timestamp = timestamp;
    this.sourceConfig = sourceConfig;
    this.sinkConfig = sinkConfig;
    this.sourceVersion = sourceVersion;
    this.sinkVersion = sinkVersion;
    this.flowID = flowID;
  }

  /** Copies existing FlumeConfigData. **/
  public FlumeConfigData(FlumeConfigData fcd) {
    this.timestamp = fcd.timestamp;
    this.sourceConfig = fcd.sourceConfig;
    this.sinkConfig = fcd.sinkConfig;
    this.sourceVersion = fcd.sourceVersion;
    this.sinkVersion = fcd.sinkVersion;
    this.flowID = fcd.flowID;
  }

  /** Empty constructor. **/
  public FlumeConfigData() {

  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public String getSourceConfig() {
    return this.sourceConfig;
  }

  public String getSinkConfig() {
    return this.sinkConfig;
  }

  public long getSourceVersion() {
    return this.sourceVersion;
  }

  public long getSinkVersion() {
    return this.sinkVersion;
  }

  public String getFlowID() {
    return this.flowID;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public void setSourceConfig(String sourceConfig) {
    this.sourceConfig = sourceConfig;
  }

  public void setSinkConfig(String sinkConfig) {
    this.sinkConfig = sinkConfig;
  }

  public void setSourceVersion(long sourceVersion) {
    this.sourceVersion = sourceVersion;
  }

  public void setSinkVersion(long sinkVersion) {
    this.sinkVersion = sinkVersion;
  }

  public void setFlowID(String flowID) {
    this.flowID = flowID;
  }
}
