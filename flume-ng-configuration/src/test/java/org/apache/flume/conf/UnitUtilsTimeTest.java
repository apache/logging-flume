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
public class UnitUtilsTimeTest {
  private String param;
  private Long result;

  public UnitUtilsTimeTest(String param, Long result) {
    this.param = param;
    this.result = result;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { 
        { "2h", 1L * 2 * 60 * 60 * 1000 }, 
        { "2H", 1L * 2 * 60 * 60 * 1000 },
        { "30m", 1L * 30 * 60 * 1000 }, 
        { "30M", 1L * 30 * 60 * 1000 }, 
        { "3s", 1L * 3 * 1000 },
        { "3S", 1L * 3 * 1000 }, 
        { "3000ms", 1L * 3000 }, 
        { "3000MS", 1L * 3000 }, 
        { "3000", 1L * 3000 },
        { "1.5h", 1L * Math.round(1.5 * 60 * 60 * 1000) },
        { "1.5h30m", 1L * Math.round(1.5 * 60 * 60 * 1000 + 30 * 60 * 1000) }, });
  }

  @Test
  public void testString2Millisecond() {
    assertEquals(result, UnitUtils.parseMillisecond(param));
  }

}
