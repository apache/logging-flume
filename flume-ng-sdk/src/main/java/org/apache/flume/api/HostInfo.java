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
package org.apache.flume.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenience class that holds the property reference name along with the
 * hostname and port number of a specified host address. It also provides
 * a method to parse out the host list from a given properties object
 * that contains the host details.
 */
public class HostInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(HostInfo.class);

  private final String referenceName;
  private final String hostName;
  private final int portNumber;

  public HostInfo(String referenceName, String hostName, int portNumber) {
    this.referenceName = referenceName;
    this.hostName = hostName;
    this.portNumber = portNumber;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getHostName() {
    return hostName;
  }

  public int getPortNumber() {
    return portNumber;
  }

  @Override
  public String toString() {
    return referenceName + "{" + hostName + ":" + portNumber + "}";
  }

  public static List<HostInfo> getHostInfoList(Properties properties) {
    List<HostInfo> hosts = new ArrayList<HostInfo>();
    String hostNames = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_HOSTS);
    String[] hostList;
    if (hostNames != null && !hostNames.isEmpty()) {
      hostList = hostNames.split("\\s+");
      for (int i = 0; i < hostList.length; i++) {
        String hostAndPortStr = properties.getProperty(
            RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + hostList[i]);
        // Ignore that host if value is not there
        if (hostAndPortStr != null) {
          String[] hostAndPort = hostAndPortStr.split(":");
          if (hostAndPort.length != 2){
            LOGGER.error("Invalid host address" + hostAndPortStr);
            throw new FlumeException("Invalid host address" + hostAndPortStr);
          }
          Integer port = null;
          try {
            port = Integer.parseInt(hostAndPort[1]);
          } catch (NumberFormatException e) {
            LOGGER.error("Invalid port number" + hostAndPortStr, e);
            throw new FlumeException("Invalid port number" + hostAndPortStr);
          }
          HostInfo info = new HostInfo(hostList[i],
              hostAndPort[0].trim(), port);
          hosts.add(info);
        }
      }
    }

    return hosts;
  }
}
