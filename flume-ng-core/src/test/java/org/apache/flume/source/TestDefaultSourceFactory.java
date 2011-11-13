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

package org.apache.flume.source;

import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultSourceFactory {

  private SourceFactory sourceFactory;

  @Before
  public void setUp() {
    sourceFactory = new DefaultSourceFactory();
  }

  @Test
  public void testRegister() {
    Assert.assertEquals(0, sourceFactory.getSourceNames().size());

    sourceFactory.register("seq", SequenceGeneratorSource.class);

    Assert.assertEquals(1, sourceFactory.getSourceNames().size());

    Assert
        .assertEquals("seq", sourceFactory.getSourceNames().iterator().next());
  }

  @Test
  public void testCreate() throws InstantiationException {
    Assert.assertEquals(0, sourceFactory.getSourceNames().size());

    sourceFactory.register("seq", SequenceGeneratorSource.class);

    Assert.assertEquals(1, sourceFactory.getSourceNames().size());

    Assert
        .assertEquals("seq", sourceFactory.getSourceNames().iterator().next());

    Source source = sourceFactory.create("seq");

    Assert.assertNotNull("Factory returned a null source", source);
    Assert.assertTrue("Source isn't an instance of SequenceGeneratorSource",
        source instanceof SequenceGeneratorSource);

    source = sourceFactory.create("i do not exist");

    Assert.assertNull("Factory returned a source it shouldn't have", source);
  }

  @Test
  public void testUnregister() {
    Assert.assertEquals(0, sourceFactory.getSourceNames().size());

    Assert.assertTrue("Registering a source returned false",
        sourceFactory.register("seq", SequenceGeneratorSource.class));

    Assert.assertEquals(1, sourceFactory.getSourceNames().size());

    Assert
        .assertEquals("seq", sourceFactory.getSourceNames().iterator().next());

    Assert.assertFalse("Unregistering an unknown source returned true",
        sourceFactory.unregister("i do not exist"));
    Assert.assertTrue("Unregistering a source returned false",
        sourceFactory.unregister("seq"));

    Assert.assertEquals(0, sourceFactory.getSourceNames().size());
  }

}
