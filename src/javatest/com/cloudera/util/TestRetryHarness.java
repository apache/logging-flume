/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.util;

import junit.framework.TestCase;

public class TestRetryHarness extends TestCase {
  /** 
   * Simple test to ensure that a RetryHarness actually does retry / fail
   * when the Retryable fails.
   */
  public void testRetryHarness() throws InterruptedException, Exception {
    Retryable retry = new Retryable() {
      int totaltries = 0;
      public boolean doTry() {
        totaltries++;
        if (totaltries < 3) {
          return false;
        }
        totaltries = 0;
        return true;
      }
    };
    
    RetryHarness harness = new RetryHarness(retry, new FixedRetryPolicy(3));
    assertTrue(harness.attempt());
    harness = new RetryHarness(retry, new FixedRetryPolicy(1));
    assertFalse(harness.attempt());
  }  
  
  /**
   * Test that abort works to give an early exit. 
   * 
   */
  public void testAbort() throws Exception {   
    class TestRetryable extends Retryable {
      public int totaltries = 0;
      public boolean doTry() throws Exception {        
        harness.doAbort();        
        totaltries++;
        return false;
      }
    };    
    TestRetryable retry = new TestRetryable();    
    RetryHarness harness = new RetryHarness(retry, new FixedRetryPolicy(3));    
    assertFalse("doTry should be false!", harness.attempt());    
    assertEquals("doTry called too many times! " + retry.totaltries, retry.totaltries, 1);    
    harness = new RetryHarness(retry, new FixedRetryPolicy(1));    
  }
  
  /**
   * Test the two variants of RetryHarness, one where exceptions are rethrown 
   * after failure, one where they are always masked.  
   */
  public void testException() {
    Exception e = null;
    Retryable retry = new Retryable() {
      public boolean doTry() throws Exception {
        harness.doAbort();
        throw new Exception();
      }
    };
    
    RetryHarness harness = new RetryHarness(retry, new FixedRetryPolicy(3), true);
    try { 
      harness.attempt(); 
    } catch (Exception e1) {
      e = e1;
    }
    assertNotNull("Expected exception not thrown!",e);
    e = null;
    harness = new RetryHarness(retry, new FixedRetryPolicy(3), false);    
    try { 
      harness.attempt(); 
    } catch (Exception e1) {
      e = e1;
    }
    assertNull("Unexpected exception thrown!",e);
  }
}
