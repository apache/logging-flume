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
import java.util.Locale;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * The purpose of this test is to guard against accidental backward
 * compatibility problem since the string representation so of
 * TransactionIsolation enum are a public interface used in configuration.
 */
public class TestTransactionIsolationLevelEnum {

  public static final String TX_READ_UNCOMMITTED = "READ_UNCOMMITTED";
  public static final String TX_READ_COMMITTED = "READ_COMMITTED";
  public static final String TX_REPEATABLE_READ = "REPEATABLE_READ";
  public static final String TX_SERIALIZABLE = "SERIALIZABLE";

  private Map<String, TransactionIsolation> enumMap =
      new HashMap<String, TransactionIsolation>();

  @Before
  public void setUp() {
    enumMap.clear();
    enumMap.put(TX_READ_UNCOMMITTED, TransactionIsolation.READ_UNCOMMITTED);
    enumMap.put(TX_READ_COMMITTED, TransactionIsolation.READ_COMMITTED);
    enumMap.put(TX_REPEATABLE_READ, TransactionIsolation.REPEATABLE_READ);
    enumMap.put(TX_SERIALIZABLE, TransactionIsolation.SERIALIZABLE);
  }

  @Test
  public void testReverseLookup() {
    for (String key : enumMap.keySet()) {
      TransactionIsolation txIsolation = enumMap.get(key);
      TransactionIsolation lookupTxIsolation =
          TransactionIsolation.valueOf(key);
      String lookupTxIsolationName = lookupTxIsolation.getName();

      Assert.assertEquals(lookupTxIsolationName, lookupTxIsolation.toString());
      Assert.assertSame(txIsolation, lookupTxIsolation);
      Assert.assertEquals(key, lookupTxIsolationName);

      TransactionIsolation lookupTxIsolation2 =
          TransactionIsolation.getByName(key.toLowerCase(Locale.ENGLISH));
      Assert.assertSame(txIsolation, lookupTxIsolation2);
    }
  }
}
