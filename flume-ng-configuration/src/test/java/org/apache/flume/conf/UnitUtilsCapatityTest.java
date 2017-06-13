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

package org.apache.flume.conf;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class UnitUtilsCapatityTest {
  private String param;
  private Long result;

  public UnitUtilsCapatityTest(String param, Long result) {
    this.param = param;
    this.result = result;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { 
        { "2g", 1L * 2 * 1024 * 1024 * 1024 }, 
        { "2G", 1L * 2 * 1024 * 1024 * 1024 },
        { "2000m", 1L * 2000 * 1024 * 1024 }, 
        { "2000M", 1L * 2000 * 1024 * 1024 }, 
        { "1000k", 1L * 1000 * 1024 },
        { "1000K", 1L * 1000 * 1024 }, { "1000", 1L * 1000 }, 
        { "1.5G", 1L * Math.round(1.5 * 1024 * 1024 * 1024) },
        { "1.38g", 1L * Math.round(1.38 * 1024 * 1024 * 1024) },
        { "1g500M", 1L * 1024 * 1024 * 1024 + 500 * 1024 * 1024 }, 
        { "20M512", 1L * 20 * 1024 * 1024 + 512 },
        { "0.5g", 1L * Math.round(0.5 * 1024 * 1024 * 1024) },
        { "0.5g0.5m", 1L * Math.round(0.5 * 1024 * 1024 * 1024 + 0.5 * 1024 * 1024) } });
  }

  @Test
  public void testString2Bytes() {
    assertEquals(result, UnitUtils.parseBytes(param));
  }

}
