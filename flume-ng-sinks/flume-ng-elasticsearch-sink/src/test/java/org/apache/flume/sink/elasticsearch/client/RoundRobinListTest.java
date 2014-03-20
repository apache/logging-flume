/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.elasticsearch.client;

import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class RoundRobinListTest {

  private RoundRobinList<String> fixture;

  @Before
  public void setUp() {
    fixture = new RoundRobinList<String>(Arrays.asList("test1", "test2"));
  }

  @Test
  public void shouldReturnNextElement() {
    assertEquals("test1", fixture.get());
    assertEquals("test2", fixture.get());
    assertEquals("test1", fixture.get());
    assertEquals("test2", fixture.get());
    assertEquals("test1", fixture.get());
  }
}
