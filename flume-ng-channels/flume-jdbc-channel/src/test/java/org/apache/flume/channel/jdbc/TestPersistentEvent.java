package org.apache.flume.channel.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.flume.Event;
import org.apache.flume.channel.jdbc.impl.PersistableEvent;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPersistentEvent {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestPersistentEvent.class);

  private static final String[] CHARS = new String[] {
    "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r",
    "s","t","u","v","w","x","y","z",
    "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R",
    "S","T","U","V","W","X","Y","Z",
    "0","1","2","3","4","5","6","7","8","9",
    "!","@","#","$","%","^","&","*","(",")",
    "[","]","{","}",":",";","\"","'",",",".","<",">","?","/","\\","|",
  };

  @Test
  public void testMarshalling() {
    Random rnd = new Random(System.currentTimeMillis());

    int nameLimit = ConfigurationConstants.HEADER_NAME_LENGTH_THRESHOLD;
    int valLimit = ConfigurationConstants.HEADER_VALUE_LENGTH_THRESHOLD;

    byte[] s1 = new byte[1];
    rnd.nextBytes(s1);
    runTest(s1, null);

    byte[] s2 = new byte[2];
    rnd.nextBytes(s2);
    runTest(s2, new HashMap<String, String>());

    byte[] s3 = new byte[ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD - 2];
    rnd.nextBytes(s3);
    Map<String, String> m3 = new HashMap<String, String>();
    m3.put(generateString(rnd, 1), generateString(rnd, 1));
    runTest(s3, m3);

    byte[] s4 = new byte[ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD - 1];
    rnd.nextBytes(s4);
    Map<String, String> m4 = new HashMap<String, String>();
    m4.put(generateString(rnd, nameLimit - 21), "w");
    m4.put(generateString(rnd, nameLimit - 2), "x");
    m4.put(generateString(rnd, nameLimit - 1), "y");
    m4.put(generateString(rnd, nameLimit), "z");
    m4.put(generateString(rnd, nameLimit + 1), "a");
    m4.put(generateString(rnd, nameLimit + 2), "b");
    m4.put(generateString(rnd, nameLimit + 21), "c");
    runTest(s4, m4);

    byte[] s5 = new byte[ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD];
    rnd.nextBytes(s5);
    Map<String, String> m5 = new HashMap<String, String>();
    m5.put("w", generateString(rnd, valLimit - 21));
    m5.put("x", generateString(rnd, valLimit - 2));
    m5.put("y", generateString(rnd, valLimit - 1));
    m5.put("z", generateString(rnd, valLimit));
    m5.put("a", generateString(rnd, valLimit + 1));
    m5.put("b", generateString(rnd, valLimit + 2));
    m5.put("c", generateString(rnd, valLimit + 21));
    runTest(s5, m5);

    byte[] s6 = new byte[ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD + 1];
    rnd.nextBytes(s6);
    Map<String, String> m6 = new HashMap<String, String>();
    m6.put(generateString(rnd, nameLimit - 21),
           generateString(rnd, valLimit - 21));
    m6.put(generateString(rnd, nameLimit - 2),
        generateString(rnd, valLimit - 2));
    m6.put(generateString(rnd, nameLimit - 1),
        generateString(rnd, valLimit - 1));
    m6.put(generateString(rnd, nameLimit),
        generateString(rnd, valLimit));
    m6.put(generateString(rnd, nameLimit + 1),
        generateString(rnd, valLimit + 1));
    m6.put(generateString(rnd, nameLimit + 2),
        generateString(rnd, valLimit + 2));
    m6.put(generateString(rnd, nameLimit + 21),
        generateString(rnd, valLimit + 21));

    runTest(s6, m6);

    byte[] s7 = new byte[ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD + 2];
    rnd.nextBytes(s7);
    runTest(s7, null);

    byte[] s8 = new byte[ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD + 27];
    rnd.nextBytes(s8);
    runTest(s8, null);
  }

  private String generateString(Random rnd, int size) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; i++) {
      int x = Math.abs(rnd.nextInt());
      int y = x % CHARS.length;
      sb.append(CHARS[y]);
    }
    System.out.println("String: " + sb);
    return sb.toString();
  }



  private void runTest(byte[] payload, Map<String, String> headers) {
    PersistableEvent pe = new PersistableEvent(new MockEvent(payload, headers));
    Assert.assertArrayEquals(payload, pe.getPayload());
    Map<String, String> h = pe.getHeaders();
    if (h == null) {
      Assert.assertTrue(headers == null || headers.size() == 0);
    } else {
      Assert.assertTrue(headers.size() == h.size());
      for (String key : h.keySet()) {
        Assert.assertTrue(headers.containsKey(key));
        String value = h.get(key);
        String expectedValue = headers.remove(key);
        Assert.assertEquals(expectedValue, value);
      }
      Assert.assertTrue(headers.size() == 0);
    }
  }



  private static class MockEvent implements Event {

    private final byte[] payload;
    private final Map<String, String> headers;

    private MockEvent(byte[] payload, Map<String, String> headers) {
      this.payload = payload;
      this.headers = headers;
    }

    @Override
    public Map<String, String> getHeaders() {
      return headers;
    }

    @Override
    public void setHeaders(Map<String, String> headers) {

    }

    @Override
    public byte[] getBody() {
      return payload;
    }

    @Override
    public void setBody(byte[] body) {

    }

  }
}
