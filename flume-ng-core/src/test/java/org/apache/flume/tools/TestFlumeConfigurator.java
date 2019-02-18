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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flume.Context;
import org.junit.Test;

import junit.framework.Assert;

public class TestFlumeConfigurator {
  private String testPrefix = "TestBean.";

  /**
   * Test configuring an int. Creates a random int greater than zero
   * and then uses FlumeBeanConfigurator to test it can be set.
   */
  @Test
  public void testIntConfiguration() {

    Map<String, String> props = new HashMap<String, String>();
    Random random = new Random();
    int intValue = random.nextInt(Integer.MAX_VALUE - 1 ) + 1;
    props.put(testPrefix + "testInt", Integer.toString(intValue));
    Context context = new Context(props);

    TestBean bean = new TestBean();
    Assert.assertEquals(0, bean.getTestInt());

    FlumeBeanConfigurator.setConfigurationFields(bean, context);
    Assert.assertEquals(intValue, bean.getTestInt());
  }

  /**
   * Test configuring an short. Creates a random short greater than zero
   * and then uses FlumeBeanConfigurator to test it can be set.
   */
  @Test
  public void testShortConfiguration() {

    Map<String, String> props = new HashMap<String, String>();
    Random random = new Random();
    short shortValue = (short)(random.nextInt(Short.MAX_VALUE - 1 ) + 1);
    props.put(testPrefix + "testShort", Short.toString(shortValue));
    Context context = new Context(props);

    TestBean bean = new TestBean();
    Assert.assertEquals(0, bean.getTestShort());

    FlumeBeanConfigurator.setConfigurationFields(bean, context);
    Assert.assertEquals(shortValue, bean.getTestShort());
  }

  /**
   * Test configuring a long. Creates a random long greater than Integer.MAX_VALUE
   * and then uses FlumeBeanConfigurator to test it can be set.
   */

  @Test
  public void testLongConfiguration() {

    Map<String, String> props = new HashMap<String, String>();
    long longValue = ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE, Long.MAX_VALUE);
    props.put(testPrefix + "testLong", Long.toString(longValue));
    Context context = new Context(props);

    TestBean bean = new TestBean();
    Assert.assertEquals(0, bean.getTestLong());

    FlumeBeanConfigurator.setConfigurationFields(bean, context);
    Assert.assertEquals(longValue, bean.getTestLong());
  }

  /**
   * Test configuring an byte. Creates a random byte greater than zero
   * and then uses FlumeBeanConfigurator to test it can be set.
   */
  @Test
  public void testByteConfiguration() {

    Map<String, String> props = new HashMap<String, String>();
    Random random = new Random();
    byte byteValue = (byte)(random.nextInt(Byte.MAX_VALUE - 1 ) + 1);
    props.put(testPrefix + "testByte", Byte.toString(byteValue));
    Context context = new Context(props);

    TestBean bean = new TestBean();
    Assert.assertEquals(0, bean.getTestByte());

    FlumeBeanConfigurator.setConfigurationFields(bean, context);
    Assert.assertEquals(byteValue, bean.getTestByte());
  }
  
  /**
   * Test configuring an boolean.
   */
  @Test
  public void testBooleanConfiguration() {

    Map<String, String> props = new HashMap<String, String>();
    props.put(testPrefix + "testBoolean", "true");
    Context context = new Context(props);

    TestBean bean = new TestBean();
    Assert.assertEquals(false, bean.getTestBoolean());

    FlumeBeanConfigurator.setConfigurationFields(bean, context);
    Assert.assertEquals(true, bean.getTestBoolean());
  }
  
  /**
   * Test configuring an double. Creates a random double
   * and then uses FlumeBeanConfigurator to test it can be set.
   */
  @Test
  public void testDoubleConfiguration() {

    Map<String, String> props = new HashMap<String, String>();
    Random random = new Random();
    double doubleValue = random.nextDouble();
    props.put(testPrefix + "testDouble", Double.toString(doubleValue));

    Context context = new Context(props);

    TestBean bean = new TestBean();
    Assert.assertEquals(0.0d, bean.getTestDouble());

    FlumeBeanConfigurator.setConfigurationFields(bean, context);
    Assert.assertEquals(doubleValue, bean.getTestDouble());
  }
  
  /**
   * Test configuring an float. Creates a random float
   * and then uses FlumeBeanConfigurator to test it can be set.
   */

  @Test
  public void testFloatConfiguration() {

    Map<String, String> props = new HashMap<String, String>();
    Random random = new Random();
    float floatValue = random.nextFloat();
    props.put(testPrefix + "testFloat", Float.toString(floatValue));
    
    Context context = new Context(props);

    TestBean bean = new TestBean();
    Assert.assertEquals(0.0f, bean.getTestFloat());

    FlumeBeanConfigurator.setConfigurationFields(bean, context);
    Assert.assertEquals(floatValue, bean.getTestFloat());
  }

  /**
   * Test configuring a String. Creates a random String (UUID in this case)
   * and then uses FlumeBeanConfigurator to test it can be set.
   */
  @Test
  public void testStringConfiguration() {

    Map<String, String> props = new HashMap<String, String>();
    String stringValue = UUID.randomUUID().toString();
    props.put(testPrefix + "testString", stringValue);
    Context context = new Context(props);

    TestBean bean = new TestBean();
    Assert.assertEquals("", bean.getTestString());

    FlumeBeanConfigurator.setConfigurationFields(bean, context);
    Assert.assertEquals(stringValue, bean.getTestString());
  }

  /**
   * Test that is is not possible to configure using private setters.
   */
  @Test
  public void testPrivateConfiguration() {

    Map<String, String> props = new HashMap<String, String>();
    Random random = new Random();
    int intValue = random.nextInt(Integer.MAX_VALUE - 1 ) + 1;
    props.put(testPrefix + "privateInt", Integer.toString(intValue));
    Context context = new Context(props);

    TestBean bean = new TestBean();
    Assert.assertEquals(0, bean.getPrivateInt());

    FlumeBeanConfigurator.setConfigurationFields(bean, context);
    Assert.assertTrue(bean.getPrivateInt() != intValue);
  }

  public class TestBean {
    private int testInt = 0;
    private short testShort = 0;
    private long testLong = 0;
    private byte testByte = 0;
    private boolean testBoolean = false;
    private float testFloat = 0f;
    private double testDouble = 0d;
    private String testString = "";
    private int privateInt = 0;

    public int getTestInt() {
      return testInt;
    }
    public void setTestInt(int testInt) {
      this.testInt = testInt;
    }
    public short getTestShort() {
      return testShort;
    }
    public void setTestShort(short testShort) {
      this.testShort = testShort;
    }
    public long getTestLong() {
      return testLong;
    }
    public void setTestLong(long testLong) {
      this.testLong = testLong;
    }
    public byte getTestByte() {
      return testByte;
    }
    public void setTestByte(byte testByte) {
      this.testByte = testByte;
    }
    public boolean getTestBoolean() {
      return testBoolean;
    }
    public void setTestBoolean(boolean testBoolean) {
      this.testBoolean = testBoolean;
    }
    public float getTestFloat() {
      return testFloat;
    }
    public void setTestFloat(float testFloat) {
      this.testFloat = testFloat;
    }
    public double getTestDouble() {
      return testDouble;
    }
    public void setTestDouble(double testDouble) {
      this.testDouble = testDouble;
    }
    public String getTestString() {
      return testString;
    }
    public void setTestString(String testString) {
      this.testString = testString;
    }
    private int getPrivateInt() {
      return privateInt;
    }
    private void setPrivateInt(int privateInt) {
      this.privateInt = privateInt;
    }

  }
  
}
