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
package org.apache.flume.source.jms;

import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

/**
 * Converts a JMS Message to an Event. It's recommended
 * that sub-classes define a static sub-class of the
 * inner Builder class to handle configuration. Alternatively,
 * the sub-class can the Configurable interface.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface JMSMessageConverter {

  public List<Event> convert(Message message) throws JMSException;

  /**
   * Implementors of JMSMessageConverter must either provide
   * a suitable builder or implement the Configurable interface.
   */
  public interface Builder {
    public JMSMessageConverter build(Context context);
  }
}
