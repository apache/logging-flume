/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.source.jms;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

public class TestDefaultJMSMessageConverter {

  private static final String TEXT = "text";
  private static final byte[] BYTES = TEXT.getBytes(Charsets.UTF_8);
  private Context context;
  private Message message;
  private Map<String, String> headers;
  private JMSMessageConverter converter;

  @Before
  public void setUp() throws Exception {
    headers = Maps.newHashMap();
    context = new Context();
    converter = new DefaultJMSMessageConverter.Builder().build(context);
  }

  void createTextMessage() throws Exception {
    TextMessage message = mock(TextMessage.class);
    when(message.getText()).thenReturn(TEXT);
    this.message = message;
  }
  void createBytesMessage() throws Exception {
    BytesMessage message = mock(BytesMessage.class);
    when(message.getBodyLength()).thenReturn((long)BYTES.length);
    when(message.readBytes(any(byte[].class))).then(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        byte[] buffer = (byte[])invocation.getArguments()[0];
        if(buffer != null) {
          assertEquals(buffer.length, BYTES.length);
          System.arraycopy(BYTES, 0, buffer, 0, BYTES.length);
        }
        return BYTES.length;
      }
    });
    this.message = message;
  }
  void createObjectMessage() throws Exception {
    ObjectMessage message = mock(ObjectMessage.class);
    when(message.getObject()).thenReturn(TEXT);
    this.message = message;
  }
  void createHeaders() throws Exception {
    final Iterator<String> keys = headers.keySet().iterator();
    when(message.getPropertyNames()).thenReturn(new Enumeration<Object>() {
      @Override
      public boolean hasMoreElements() {
        return keys.hasNext();
      }
      @Override
      public Object nextElement() {
        return keys.next();
      }
    });
    when(message.getStringProperty(anyString())).then(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        return headers.get(invocation.getArguments()[0]);
      }
    });
  }

  @Test
  public void testTextMessage() throws Exception {
    createTextMessage();
    headers.put("key1", "value1");
    headers.put("key2", "value2");
    createHeaders();
    Event event = converter.convert(message).iterator().next();
    assertEquals(headers, event.getHeaders());
    assertEquals(TEXT, new String(event.getBody(), Charsets.UTF_8));
  }
  @Test
  public void testBytesMessage() throws Exception {
    createBytesMessage();
    headers.put("key1", "value1");
    headers.put("key2", "value2");
    createHeaders();
    Event event = converter.convert(message).iterator().next();
    assertEquals(headers, event.getHeaders());
    assertArrayEquals(BYTES, event.getBody());
  }
  @Test(expected = JMSException.class)
  public void testBytesMessageTooLarge() throws Exception {
    createBytesMessage();
    when(((BytesMessage)message).getBodyLength()).thenReturn(Long.MAX_VALUE);
    createHeaders();
    converter.convert(message);
  }
  @Test(expected = JMSException.class)
  public void testBytesMessagePartialReturn() throws Exception {
    createBytesMessage();
    when(((BytesMessage)message).readBytes(any(byte[].class)))
      .thenReturn(BYTES.length + 1);
    createHeaders();
    converter.convert(message);
  }

  @Test
  public void testObjectMessage() throws Exception {
    createObjectMessage();
    headers.put("key1", "value1");
    headers.put("key2", "value2");
    createHeaders();
    Event event = converter.convert(message).iterator().next();
    assertEquals(headers, event.getHeaders());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = new ObjectOutputStream(bos);
    out.writeObject(TEXT);
    assertArrayEquals(bos.toByteArray(), event.getBody());
  }

  @Test
  public void testNoHeaders() throws Exception {
    createTextMessage();
    createHeaders();
    Event event = converter.convert(message).iterator().next();
    assertEquals(Collections.EMPTY_MAP, event.getHeaders());
    assertEquals(TEXT, new String(event.getBody(), Charsets.UTF_8));
  }
}
