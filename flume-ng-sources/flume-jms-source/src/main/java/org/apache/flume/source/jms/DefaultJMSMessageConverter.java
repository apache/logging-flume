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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.SimpleEvent;

/**
 * <p>Converts BytesMessage, TextMessage, and ObjectMessage
 * to a Flume Event. All Message Property names are added
 * as headers to the Event. The conversion of the body is
 * as follows:</p>
 *
 * <p><strong>BytesMessage:</strong> Body from message is
 * set as the body of the Event.</p>
 * <p><strong>TextMessage:</strong> String body converted to a byte
 * array byte getBytes(charset). Charset defaults to UTF-8 but can be
 * configured.</p>
 * <p><strong>ObjectMessage:</strong> Object is written to
 *  an ByteArrayOutputStream wrapped by an ObjectOutputStream
 *  and the resulting byte array is the body of the message.</p>
 */
public class DefaultJMSMessageConverter implements JMSMessageConverter {

  private final Charset charset;

  private DefaultJMSMessageConverter(String charset) {
    this.charset = Charset.forName(charset);
  }

  public static class Builder implements JMSMessageConverter.Builder {
    @Override
    public JMSMessageConverter build(Context context) {
      return new DefaultJMSMessageConverter(context.getString(
          JMSSourceConfiguration.CONVERTER_CHARSET,
          JMSSourceConfiguration.CONVERTER_CHARSET_DEFAULT).trim());
    }
  }

  @Override
  public List<Event> convert(Message message) throws JMSException {
    Event event = new SimpleEvent();
    Map<String, String> headers = event.getHeaders();
    @SuppressWarnings("rawtypes")
    Enumeration propertyNames = message.getPropertyNames();
    while (propertyNames.hasMoreElements()) {
      String name = propertyNames.nextElement().toString();
      String value = message.getStringProperty(name);
      headers.put(name, value);
    }
    if (message instanceof BytesMessage) {
      BytesMessage bytesMessage = (BytesMessage)message;
      long length = bytesMessage.getBodyLength();
      if (length > 0L) {
        if (length > Integer.MAX_VALUE) {
          throw new JMSException("Unable to process message " + "of size "
              + length);
        }
        byte[] body = new byte[(int)length];
        int count = bytesMessage.readBytes(body);
        if (count != length) {
          throw new JMSException("Unable to read full message. " +
              "Read " + count + " of total " + length);
        }
        event.setBody(body);
      }
    } else if (message instanceof TextMessage) {
      TextMessage textMessage = (TextMessage)message;
      String text = textMessage.getText();
      if (text != null) {
        event.setBody(text.getBytes(charset));
      }
    } else if (message instanceof ObjectMessage) {
      ObjectMessage objectMessage = (ObjectMessage)message;
      Object object = objectMessage.getObject();
      if (object != null) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
          out = new ObjectOutputStream(bos);
          out.writeObject(object);
          event.setBody(bos.toByteArray());
        } catch (IOException e) {
          throw new FlumeException("Error serializing object", e);
        } finally {
          try {
            if (out != null) {
              out.close();
            }
          } catch (IOException e) {
            throw new FlumeException("Error closing ObjectOutputStream", e);
          }
          try {
            if (bos != null) {
              bos.close();
            }
          } catch (IOException e) {
            throw new FlumeException("Error closing ByteArrayOutputStream", e);
          }
        }
      }

    }
    List<Event> events = new ArrayList<Event>(1);
    events.add(event);
    return events;
  }
}
