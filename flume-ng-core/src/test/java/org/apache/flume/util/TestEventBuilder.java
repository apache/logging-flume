package org.apache.flume.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestEventBuilder {

  @Test
  public void testBody() {
    Event<String> e1 = EventBuilder.withBody("e1");
    Assert.assertNotNull(e1);
    Assert.assertEquals("body is correct", "e1", e1.getBody());

    Event<Long> e2 = EventBuilder.withBody(2L);
    Assert.assertNotNull(e2);
    Assert.assertEquals("body is correct", Long.valueOf(2L), e2.getBody());
  }

  @Test
  public void testHeaders() {
    Map<String, String> headers = new HashMap<String, String>();

    headers.put("one", "1");
    headers.put("two", "2");

    Event<?> e1 = EventBuilder.withBody("e1", headers);

    Assert.assertNotNull(e1);
    Assert.assertEquals("e1 has the proper body", "e1", e1.getBody());
    Assert.assertEquals("e1 has the proper headers", 2, e1.getHeaders().size());
    Assert.assertEquals("e1 has a one key", "1", e1.getHeaders().get("one"));
  }

}
