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
package org.apache.flume.serialization;

import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestBodyTextEventSerializer {

  File testFile = new File("src/test/resources/events.txt");

  @Test
  public void test() throws FileNotFoundException, IOException {

    OutputStream out = new FileOutputStream(testFile);
    EventSerializer serializer =
        EventSerializerFactory.getInstance("text", null, out);
    serializer.afterCreate();
    serializer.write(EventBuilder.withBody("event 1", Charsets.UTF_8));
    serializer.write(EventBuilder.withBody("event 2", Charsets.UTF_8));
    serializer.write(EventBuilder.withBody("event 3", Charsets.UTF_8));
    serializer.flush();
    serializer.beforeClose();
    out.flush();
    out.close();

    BufferedReader reader = new BufferedReader(new FileReader(testFile));
    String line;
    int num = 0;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
      num++;
    }

    Assert.assertEquals(3, num);

    FileUtils.forceDelete(testFile);
  }

}
