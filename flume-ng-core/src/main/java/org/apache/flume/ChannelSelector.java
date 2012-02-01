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
package org.apache.flume;

import java.util.List;

import org.apache.flume.conf.Configurable;

/**
 * <p>
 * Allows the selection of a subset of channels from the given set based on
 * its implementation policy. Different implementations of this interface
 * embody different policies that affect the choice of channels that a source
 * will push the incoming events to.
 * </p>
 */
public interface ChannelSelector extends NamedComponent, Configurable {

  /**
   * @param channels all channels the selector could select from.
   */
  public void setChannels(List<Channel> channels);

  /**
   * Returns a list of required channels. A failure in writing the event to
   * these channels must be communicated back to the source that received this
   * event.
   * @param event
   * @return the list of required channels that this selector has selected for
   * the given event.
   */
  public List<Channel> getRequiredChannels(Event event);


  /**
   * Returns a list of optional channels. A failure in writing the event to
   * these channels must be ignored.
   * @param event
   * @return the list of optional channels that this selector has selected for
   * the given event.
   */
  public List<Channel> getOptionalChannels(Event event);

  /**
   * @return the list of all channels that this selector is configured to work
   * with.
   */
  public List<Channel> getAllChannels();

}
