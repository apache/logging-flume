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
 * compatibility problem since the string representation so of DatabaseType enum
 * are a public interface used in configuration.
 */
public class TestDatabaseTypeEnum {

  public static final String DBTYPE_OTHER = "OTHER";
  public static final String DBTYPE_DERBY = "DERBY";
  public static final String DBTYPE_MYSQL = "MYSQL";
  public static final String DBTYPE_PGSQL = "POSTGRESQL";
  public static final String DBTYPE_ORACLE = "ORACLE";

  private Map<String, DatabaseType> enumMap =
      new HashMap<String, DatabaseType>();

  @Before
  public void setUp() {
    enumMap.clear();
    enumMap.put(DBTYPE_OTHER, DatabaseType.OTHER);
    enumMap.put(DBTYPE_DERBY, DatabaseType.DERBY);
    enumMap.put(DBTYPE_MYSQL, DatabaseType.MYSQL);
    enumMap.put(DBTYPE_PGSQL, DatabaseType.POSTGRESQL);
    enumMap.put(DBTYPE_ORACLE, DatabaseType.ORACLE);
  }

  @Test
  public void testDatabaseTypeLookup() {
    for (String key : enumMap.keySet()) {
      DatabaseType type = enumMap.get(key);
      DatabaseType lookupType = DatabaseType.valueOf(key);
      String lookupTypeName = lookupType.getName();

      Assert.assertEquals(lookupTypeName, lookupType.toString());
      Assert.assertSame(type, lookupType);
      Assert.assertEquals(key, lookupTypeName);

      DatabaseType lookupType2 = DatabaseType.getByName(key.toLowerCase(Locale.ENGLISH));
      Assert.assertSame(type, lookupType2);
    }
  }

  @Test
  public void testUnknonwnDatabaseTypeLookup() {
    String[] invalidTypes = new String[] { "foo", "bar", "abcd" };

    for (String key : invalidTypes) {
      DatabaseType type = DatabaseType.getByName(key);

      Assert.assertSame(type, DatabaseType.OTHER);
    }
  }
}
