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

package org.apache.flume.tools;

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;

/**
 * Utility class to enable runtime configuration of Java objects using provided
 * Flume context objects (or equivalent). The methods use reflection to identify
 * Fields on the configurable object and then looks for matching properties in
 * the provided properties bundle.
 *
 */
public class FlumeBeanConfigurator {

  /**
   * Utility method that will set properties on a Java bean (<code>Object configurable</code>)
   * based on the provided <code>properties</code> bundle.
   * If there is a type issue, or an access problem
   * then a <code>ConfigurationException</code> will be thrown.
   *
   * @param configurable Any properties must be modifiable via setter methods.
   * @param properties Map&lt;String, String&gt;
   * @throws ConfigurationException
   */
  public static void setConfigurationFields(Object configurable, Map<String, String> properties)
      throws ConfigurationException {
    Class<?> clazz = configurable.getClass();

    for (Method method : clazz.getMethods()) {
      String methodName = method.getName();
      if (methodName.startsWith("set") && method.getParameterTypes().length == 1) {
        String fieldName = methodName.substring(3);

        String value = properties.get(StringUtils.uncapitalize(fieldName));
        if (value != null) {

          Class<?> fieldType = method.getParameterTypes()[0];;
          try {
            if (fieldType.equals(String.class)) {
              method.invoke(configurable, value);
            } else if (fieldType.equals(boolean.class)) {
              method.invoke(configurable, Boolean.parseBoolean(value));
            } else if (fieldType.equals(short.class)) {
              method.invoke(configurable, Short.parseShort(value));
            } else if (fieldType.equals(long.class)) {
              method.invoke(configurable, Long.parseLong(value));
            } else if (fieldType.equals(float.class)) {
              method.invoke(configurable, Float.parseFloat(value));
            } else if (fieldType.equals(int.class)) {
              method.invoke(configurable, Integer.parseInt(value));
            } else if (fieldType.equals(double.class)) {
              method.invoke(configurable, Double.parseDouble(value));
            } else if (fieldType.equals(char.class)) {
              method.invoke(configurable, value.charAt(0));
            } else if (fieldType.equals(byte.class)) {
              method.invoke(configurable, Byte.parseByte(value));
            } else if (fieldType.equals(String[].class)) {
              method.invoke(configurable, (Object)value.split("\\s+"));
            } else {
              throw new ConfigurationException(
                  "Unable to configure component due to an unsupported type on field: " 
              + fieldName);
            }
          } catch (Exception ex) {
            if (ex instanceof ConfigurationException) {
              throw (ConfigurationException)ex;
            } else {
              throw new ConfigurationException("Unable to configure component: ", ex);
            }
          }
        }
      }
    }
  }

  /**
   * Utility method that will set properties on a Java bean (<code>Object configurable</code>)
   * based on the provided <code>Context</code>.
   * N.B. This method will take the Flume Context and look for sub-properties named after the
   * class name of the <code>configurable</code> object.
   * If there is a type issue, or an access problem
   * then a <code>ConfigurationException</code> will be thrown.
   *
   * @param configurable Any properties must be modifiable via setter methods.
   * @param context
   * @throws ConfigurationException
   */
  public static void setConfigurationFields(Object configurable, Context context)
                        throws ConfigurationException {
    Class<?> clazz = configurable.getClass();
    Map<String, String> properties = context.getSubProperties(clazz.getSimpleName() + ".");
    setConfigurationFields(configurable, properties);
  }

  /**
   * Utility method that will set properties on a Java bean (<code>Object configurable</code>)
   * based on the provided <code>Context</code>.
   * N.B. This method will take the Flume Context and look for sub-properties named after the
   * <code>subPropertiesPrefix</code> String.
   * If there is a type issue, or an access problem
   * then a <code>ConfigurationException</code> will be thrown.
   *
   * @param configurable Object: Any properties must be modifiable via setter methods.
   * @param context org.apache.flume.Context;
   * @param subPropertiesPrefix String
   * @throws ConfigurationException
   */
  public static void setConfigurationFields(Object configurable, Context context,
      String subPropertiesPrefix) throws ConfigurationException {
    Map<String, String> properties = context.getSubProperties(subPropertiesPrefix);
    setConfigurationFields(configurable, properties);
  }
}
