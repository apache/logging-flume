/**
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
package org.apache.flume.channel.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MockEventUtils {

  public static final Logger LOGGER =
      LoggerFactory.getLogger(MockEventUtils.class);

  private static final Random RANDOM = new Random(System.currentTimeMillis());

  private static final String[] CHARS = new String[] {
    "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r",
    "s","t","u","v","w","x","y","z",
    "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R",
    "S","T","U","V","W","X","Y","Z",
    "0","1","2","3","4","5","6","7","8","9",
    "!","@","#","$","%","^","&","*","(",")",
    "[","]","{","}",":",";","\"","'",",",".","<",">","?","/","\\","|",
  };

  public static byte[] generatePayload(int size) {
    byte[] result = new byte[size];
    RANDOM.nextBytes(result);
    return result;
  }

  public static String generateHeaderString(int size) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; i++) {
      int x = Math.abs(RANDOM.nextInt());
      int y = x % CHARS.length;
      sb.append(CHARS[y]);
    }
    return sb.toString();
  }

  /**
   * Generates a mock event using the specified margins that are offset from
   * the threshold values of the various sizes. Also the number of headers is
   * specified along with number of channels. The last parameter - numChannels
   * is used to calculate a channel name that will be used to tag the event
   * with.
   * @param payloadMargin
   * @param headerNameMargin
   * @param headerValueMargin
   * @param numHeaders
   * @param numChannels
   * @return
   */
  public static MockEvent generateMockEvent(int payloadMargin,
      int headerNameMargin,	int headerValueMargin, int numHeaders,
      int numChannels) {

    int chIndex = 0;
    if (numChannels > 1) {
      chIndex = Math.abs(RANDOM.nextInt())%numChannels;
    }
    String channel = "test-"+chIndex;

    StringBuilder sb = new StringBuilder("New Event[payload size:");

    int plTh = ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD;
    int plSize = Math.abs(RANDOM.nextInt())%plTh + payloadMargin;
    sb.append(plSize).append(", numHeaders:").append(numHeaders);
    sb.append(", channel:").append(channel);

    byte[] payload = generatePayload(plSize);
    int nmTh = ConfigurationConstants.HEADER_NAME_LENGTH_THRESHOLD;
    int vlTh = ConfigurationConstants.HEADER_VALUE_LENGTH_THRESHOLD;

    Map<String, String> headers = new HashMap<String, String>();
    for (int i = 0; i < numHeaders; i++) {
      int nmSize = Math.abs(RANDOM.nextInt())%nmTh + headerNameMargin;
      int vlSize = Math.abs(RANDOM.nextInt())%vlTh + headerValueMargin;

      String name = generateHeaderString(nmSize);
      String value = generateHeaderString(vlSize);

      headers.put(name, value);
      sb.append("{nm:").append(nmSize).append(",vl:").append(vlSize);
      sb.append("} ");
    }

    LOGGER.debug(sb.toString());

    return new MockEvent(payload, headers, channel);
  }

  public static int generateSleepInterval(int upperBound) {
    return Math.abs(RANDOM.nextInt(upperBound));
  }

  private MockEventUtils() {
    // Disable explicit object creation
  }
}
