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
package org.apache.flume.channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;

public class TestMultiplexingChannelSelector {

  private List<Channel> channels = new ArrayList<Channel>();

  private ChannelSelector selector;
  private Map<String, String> config = new HashMap<String, String>();

  @Before
  public void setUp() throws Exception {
    channels.clear();
    channels.add(MockChannel.createMockChannel("ch1"));
    channels.add(MockChannel.createMockChannel("ch2"));
    channels.add(MockChannel.createMockChannel("ch3"));
    config.put("type", "multiplexing");
    config.put("header", "myheader");

    config.put("optional.foo", "ch2 ch3");
    config.put("optional.xyz", "ch1 ch3");
    config.put("optional.zebra", "ch1 ch2");


  }

  @Test
  public void testSelection() throws Exception {

    config.put("mapping.foo", "ch1 ch2");
    config.put("mapping.bar", "ch2 ch3");
    config.put("mapping.xyz", "ch1 ch2 ch3");
    config.put("default", "ch1 ch3");
    selector = ChannelSelectorFactory.create(channels, config);
    Assert.assertTrue(selector instanceof MultiplexingChannelSelector);

    Event event1 = new MockEvent();
    Map<String, String> header1 = new HashMap<String, String>();
    header1.put("myheader", "foo");// should match ch1 ch2
    event1.setHeaders(header1);

    List<Channel> reqCh1 = selector.getRequiredChannels(event1);
    Assert.assertEquals(2, reqCh1.size());
    Assert.assertTrue(reqCh1.get(0).getName().equals("ch1"));
    Assert.assertTrue(reqCh1.get(1).getName().equals("ch2"));
    List<Channel> optCh1 = selector.getOptionalChannels(event1);
    Assert.assertTrue(optCh1.size() == 1);
    //ch2 should not be there -- since it is a required channel
    Assert.assertTrue(optCh1.get(0).getName().equals("ch3"));



    Event event2 = new MockEvent();
    Map<String, String> header2 = new HashMap<String, String>();
    header2.put("myheader", "bar"); // should match ch2 ch3
    event2.setHeaders(header2);

    List<Channel> reqCh2 = selector.getRequiredChannels(event2);
    Assert.assertEquals(2, reqCh2.size());
    Assert.assertTrue(reqCh2.get(0).getName().equals("ch2"));
    Assert.assertTrue(reqCh2.get(1).getName().equals("ch3"));
    List<Channel> optCh2 = selector.getOptionalChannels(event2);
    Assert.assertTrue(optCh2.isEmpty());

    Event event3 = new MockEvent();
    Map<String, String> header3 = new HashMap<String, String>();
    header3.put("myheader", "xyz"); // should match ch1 ch2 ch3
    event3.setHeaders(header3);

    List<Channel> reqCh3 = selector.getRequiredChannels(event3);
    Assert.assertEquals(3, reqCh3.size());
    Assert.assertTrue(reqCh3.get(0).getName().equals("ch1"));
    Assert.assertTrue(reqCh3.get(1).getName().equals("ch2"));
    Assert.assertTrue(reqCh3.get(2).getName().equals("ch3"));
    List<Channel> optCh3 = selector.getOptionalChannels(event3);
    //All of the optional channels should go away.
    Assert.assertTrue(optCh3.size() == 0);

  }

  //If the header information cannot map the event to any of the channels
  //it should always be mapped to the default channel(s).
  @Test
  public void testNoSelection() throws Exception {

    config.put("mapping.foo", "ch1 ch2");
    config.put("mapping.bar", "ch2 ch3");
    config.put("mapping.xyz", "ch1 ch2 ch3");
    config.put("default", "ch1 ch3");
    selector = ChannelSelectorFactory.create(channels, config);
    Assert.assertTrue(selector instanceof MultiplexingChannelSelector);
    Event noHeaderEvent = new MockEvent();

    List<Channel> reqCh1 = selector.getRequiredChannels(noHeaderEvent);
    List<Channel> optCh1 = selector.getOptionalChannels(noHeaderEvent);
    Assert.assertEquals(2, reqCh1.size());
    Assert.assertTrue(reqCh1.get(0).getName().equals("ch1"));
    Assert.assertTrue(reqCh1.get(1).getName().equals("ch3"));
    Assert.assertTrue(optCh1.isEmpty());

    Map<String, String> header2 = new HashMap<String, String>();
    header2.put("someheader", "foo");
    Event invalidHeaderEvent = new MockEvent();
    invalidHeaderEvent.setHeaders(header2);

    List<Channel> reqCh2 = selector.getRequiredChannels(invalidHeaderEvent);
    List<Channel> optCh2 = selector.getOptionalChannels(invalidHeaderEvent);
    Assert.assertEquals(2, reqCh2.size());
    Assert.assertTrue(reqCh2.get(0).getName().equals("ch1"));
    Assert.assertTrue(reqCh2.get(1).getName().equals("ch3"));
    Assert.assertTrue(optCh2.isEmpty());

    Map<String, String> header3 = new HashMap<String, String>();
    header3.put("myheader", "bar1");
    Event unmatchedHeaderEvent = new MockEvent();
    unmatchedHeaderEvent.setHeaders(header3);

    List<Channel> reqCh3 = selector.getRequiredChannels(unmatchedHeaderEvent);
    List<Channel> optCh3 = selector.getOptionalChannels(unmatchedHeaderEvent);
    Assert.assertEquals(2, reqCh3.size());
    Assert.assertTrue(reqCh3.get(0).getName().equals("ch1"));
    Assert.assertTrue(reqCh3.get(1).getName().equals("ch3"));
    Assert.assertTrue(optCh3.isEmpty());

    Map<String, String> header4 = new HashMap<String, String>();
    header4.put("myheader", "zebra");
    Event zebraEvent = new MockEvent();
    zebraEvent.setHeaders(header4);

    List<Channel> reqCh4 = selector.getRequiredChannels(zebraEvent);
    List<Channel> optCh4 = selector.getOptionalChannels(zebraEvent);
    Assert.assertEquals(2, reqCh4.size());
    Assert.assertTrue(reqCh4.get(0).getName().equals("ch1"));
    Assert.assertTrue(reqCh4.get(1).getName().equals("ch3"));
    //Since ch1 is also in default list, it is removed.
    Assert.assertTrue(optCh4.size() == 1);
    Assert.assertTrue(optCh4.get(0).getName().equals("ch2"));

    List<Channel> allChannels = selector.getAllChannels();
    Assert.assertTrue(allChannels.size() == 3);
    Assert.assertTrue(allChannels.get(0).getName().equals("ch1"));
    Assert.assertTrue(allChannels.get(1).getName().equals("ch2"));
    Assert.assertTrue(allChannels.get(2).getName().equals("ch3"));
  }

  @Test
  public void testNoDefault() {

    config.put("mapping.foo", "ch1 ch2");
    config.put("mapping.bar", "ch2 ch3");
    config.put("mapping.xyz", "ch1 ch2 ch3");
    config.put("mapping.zebra", "ch2");
    config.put("optional.zebra", "ch1 ch3");
    selector = ChannelSelectorFactory.create(channels, config);
    Assert.assertTrue(selector instanceof MultiplexingChannelSelector);

    Event event1 = new MockEvent();
    Map<String, String> header1 = new HashMap<String, String>();
    header1.put("myheader", "foo");// should match ch1 ch2
    event1.setHeaders(header1);

    List<Channel> reqCh1 = selector.getRequiredChannels(event1);
    Assert.assertEquals(2, reqCh1.size());
    Assert.assertEquals("ch1", reqCh1.get(0).getName());
    Assert.assertEquals("ch2", reqCh1.get(1).getName());
    List<Channel> optCh1 = selector.getOptionalChannels(event1);
    Assert.assertTrue(optCh1.size() == 1);
    //ch2 should not be there -- since it is a required channel
    Assert.assertEquals("ch3", optCh1.get(0).getName());



    Event event2 = new MockEvent();
    Map<String, String> header2 = new HashMap<String, String>();
    header2.put("myheader", "bar"); // should match ch2 ch3
    event2.setHeaders(header2);

    List<Channel> reqCh2 = selector.getRequiredChannels(event2);
    Assert.assertEquals(2, reqCh2.size());
    Assert.assertEquals("ch2", reqCh2.get(0).getName());
    Assert.assertEquals("ch3", reqCh2.get(1).getName());
    List<Channel> optCh2 = selector.getOptionalChannels(event2);
    Assert.assertTrue(optCh2.isEmpty());

    Event event3 = new MockEvent();
    Map<String, String> header3 = new HashMap<String, String>();
    header3.put("myheader", "xyz"); // should match ch1 ch2 ch3
    event3.setHeaders(header3);

    List<Channel> reqCh3 = selector.getRequiredChannels(event3);
    Assert.assertEquals(3, reqCh3.size());
    Assert.assertEquals("ch1", reqCh3.get(0).getName());
    Assert.assertEquals("ch2", reqCh3.get(1).getName());
    Assert.assertEquals("ch3", reqCh3.get(2).getName());
    List<Channel> optCh3 = selector.getOptionalChannels(event3);
    //All of the optional channels should go away.
    Assert.assertTrue(optCh3.isEmpty());

    Event event4 = new MockEvent();
    Map<String, String> header4 = new HashMap<String, String>();
    header4.put("myheader", "zebra");
    event4.setHeaders(header4);

    List<Channel> reqCh4 = selector.getRequiredChannels(event4);
    Assert.assertEquals(1, reqCh4.size());
    Assert.assertEquals("ch2", reqCh4.get(0).getName());
    List<Channel> optCh4 = selector.getOptionalChannels(event4);
    Assert.assertEquals(2, optCh4.size());
    Assert.assertEquals("ch1", optCh4.get(0).getName());
    Assert.assertEquals("ch3", optCh4.get(1).getName());
  }

  @Test
  public void testNoMandatory() {

    config.put("default", "ch3");
    config.put("optional.foo", "ch1 ch2");
    config.put("optional.zebra", "ch2 ch3");
    selector = ChannelSelectorFactory.create(channels, config);
    Assert.assertTrue(selector instanceof MultiplexingChannelSelector);

    Event event1 = new MockEvent();
    Map<String, String> header1 = new HashMap<String, String>();
    header1.put("myheader", "foo");// should match ch1 ch2
    event1.setHeaders(header1);

    List<Channel> reqCh1 = selector.getRequiredChannels(event1);
    Assert.assertEquals(1, reqCh1.size());
    Assert.assertEquals("ch3", reqCh1.get(0).getName());
    List<Channel> optCh1 = selector.getOptionalChannels(event1);
    Assert.assertEquals(2, optCh1.size());
    //ch2 should not be there -- since it is a required channel
    Assert.assertEquals("ch1", optCh1.get(0).getName());
    Assert.assertEquals("ch2", optCh1.get(1).getName());

    Event event4 = new MockEvent();
    Map<String, String> header4 = new HashMap<String, String>();
    header4.put("myheader", "zebra");
    event4.setHeaders(header4);

    List<Channel> reqCh4 = selector.getRequiredChannels(event4);
    Assert.assertEquals(1, reqCh4.size());
    Assert.assertTrue(reqCh4.get(0).getName().equals("ch3"));
    List<Channel> optCh4 = selector.getOptionalChannels(event4);
    //ch3 was returned as a required channel, because it is default.
    //So it is not returned in optional
    Assert.assertEquals(1, optCh4.size());
    Assert.assertEquals("ch2", optCh4.get(0).getName());

  }

  @Test
  public void testOnlyOptional() {
    config.put("optional.foo", "ch1 ch2");
    config.put("optional.zebra", "ch2 ch3");
    selector = ChannelSelectorFactory.create(channels, config);
    Assert.assertTrue(selector instanceof MultiplexingChannelSelector);

    Event event1 = new MockEvent();
    Map<String, String> header1 = new HashMap<String, String>();
    header1.put("myheader", "foo");// should match ch1 ch2
    event1.setHeaders(header1);

    List<Channel> reqCh1 = selector.getRequiredChannels(event1);
    Assert.assertTrue(reqCh1.isEmpty());
    List<Channel> optCh1 = selector.getOptionalChannels(event1);
    Assert.assertEquals(2,optCh1.size());
    //ch2 should not be there -- since it is a required channel


    Event event4 = new MockEvent();
    Map<String, String> header4 = new HashMap<String, String>();
    header4.put("myheader", "zebra");
    event4.setHeaders(header4);

    List<Channel> reqCh4 = selector.getRequiredChannels(event4);
    Assert.assertTrue(reqCh4.isEmpty());
    List<Channel> optCh4 = selector.getOptionalChannels(event4);
    Assert.assertEquals(2, optCh4.size());
    Assert.assertEquals("ch2", optCh4.get(0).getName());
    Assert.assertEquals("ch3", optCh4.get(1).getName());

  }
}
