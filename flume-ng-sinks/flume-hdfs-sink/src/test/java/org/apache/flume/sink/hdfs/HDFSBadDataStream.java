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

import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.flume.sink.hdfs.HDFSSequenceFile;


public class HDFSBadDataStream extends HDFSDataStream {
  public class HDFSBadSeqWriter extends HDFSSequenceFile {
    @Override
    public void append(Event e, FlumeFormatter fmt) throws IOException {

      if (e.getHeaders().containsKey("fault")) {
        throw new IOException("Injected fault");
      } else if (e.getHeaders().containsKey("slow")) {
        long waitTime = Long.parseLong(e.getHeaders().get("slow"));
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException eT) {
          throw new IOException("append interrupted", eT);
        }
      }
      super.append(e, fmt);
    }

  }

}
