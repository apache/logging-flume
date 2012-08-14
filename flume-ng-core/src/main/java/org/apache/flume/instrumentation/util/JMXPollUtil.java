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
package org.apache.flume.instrumentation.util;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class JMXPollUtil {

  private static Logger LOG = LoggerFactory.getLogger(JMXPollUtil.class);
  private static MBeanServer mbeanServer = ManagementFactory.
          getPlatformMBeanServer();

  public static Map<String, Map<String, String>> getAllMBeans() {
    Map<String, Map<String, String>> mbeanMap = Maps.newHashMap();
    Set<ObjectInstance> queryMBeans = null;
    try {
      queryMBeans = mbeanServer.queryMBeans(null, null);
    } catch (Exception ex) {
      LOG.error("Could not get Mbeans for monitoring", ex);
      Throwables.propagate(ex);
    }
    for (ObjectInstance obj : queryMBeans) {
      try {
        if (!obj.getObjectName().toString().startsWith("org.apache.flume")) {
          continue;
        }
        MBeanAttributeInfo[] attrs = mbeanServer.
                getMBeanInfo(obj.getObjectName()).getAttributes();
        String strAtts[] = new String[attrs.length];
        for (int i = 0; i < strAtts.length; i++) {
          strAtts[i] = attrs[i].getName();
        }
        AttributeList attrList = mbeanServer.getAttributes(
                obj.getObjectName(), strAtts);
        String component = obj.getObjectName().toString().substring(
                obj.getObjectName().toString().indexOf('=') + 1);
        Map<String, String> attrMap = Maps.newHashMap();


        for (Object attr : attrList) {
          Attribute localAttr = (Attribute) attr;
          if(localAttr.getName().equalsIgnoreCase("type")){
            component = localAttr.getValue()+ "." + component;
          }
          attrMap.put(localAttr.getName(), localAttr.getValue().toString());
        }
        mbeanMap.put(component, attrMap);
      } catch (Exception e) {
        LOG.error("Unable to poll JMX for metrics.", e);
      }
    }
    return mbeanMap;
  }
}
