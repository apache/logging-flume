/**
+ * Licensed to the Apache Software Foundation (ASF) under one
+ * or more contributor license agreements.  See the NOTICE file
+ * distributed with this work for additional information
+ * regarding copyright ownership.  The ASF licenses this file
+ * to you under the Apache License, Version 2.0 (the
+ * "License"); you may not use this file except in compliance
+ * with the License.  You may obtain a copy of the License at
+ *
+ *     http://www.apache.org/licenses/LICENSE-2.0
+ *
+ * Unless required by applicable law or agreed to in writing, software
+ * distributed under the License is distributed on an "AS IS" BASIS,
+ * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+ * See the License for the specific language governing permissions and
+ * limitations under the License.
+ */
package org.apache.flume.sink.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockFsDataOutputStream extends FSDataOutputStream{

  private static final Logger logger =
      LoggerFactory.getLogger(MockFsDataOutputStream.class);

  int currentCloseAttempts = 0;
  int numberOfClosesRequired;

  public MockFsDataOutputStream(FSDataOutputStream wrapMe,
    int numberOfClosesRequired)
      throws IOException {
    super(wrapMe.getWrappedStream(), null);

    this.numberOfClosesRequired = numberOfClosesRequired;

  }

  @Override
  public void close() throws IOException {
    currentCloseAttempts++;
    logger.info(
      "Attempting to Close: '" + currentCloseAttempts + "' of '" +
        numberOfClosesRequired + "'");
    if (currentCloseAttempts >= numberOfClosesRequired ||
      numberOfClosesRequired == 0) {
      logger.info("closing file");
      super.close();
    } else {
      throw new IOException("MockIOException");
    }
  }

  public int getCurrentCloseAttempts() {
    return currentCloseAttempts;
  }
}
