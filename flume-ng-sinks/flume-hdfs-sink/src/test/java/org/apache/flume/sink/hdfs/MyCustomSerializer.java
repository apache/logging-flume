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

package org.apache.flume.sink.hdfs;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.Arrays;

public class MyCustomSerializer implements SequenceFileSerializer {

  @Override
  public Class<LongWritable> getKeyClass() {
    return LongWritable.class;
  }

  @Override
  public Class<BytesWritable> getValueClass() {
    return BytesWritable.class;
  }

  @Override
  public Iterable<Record> serialize(Event e) {
    return Arrays.asList(
        new Record(new LongWritable(1234L), new BytesWritable(new byte[10])),
        new Record(new LongWritable(4567L), new BytesWritable(new byte[20]))
    );
  }

  public static class Builder implements SequenceFileSerializer.Builder {

    @Override
    public SequenceFileSerializer build(Context context) {
      return new MyCustomSerializer();
    }

  }

}
