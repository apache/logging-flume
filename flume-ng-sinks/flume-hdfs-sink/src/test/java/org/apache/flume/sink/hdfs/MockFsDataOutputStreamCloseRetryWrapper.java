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
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockFsDataOutputStreamCloseRetryWrapper extends FSDataOutputStream{

  private static final Logger logger =
      LoggerFactory.getLogger(MockFsDataOutputStreamCloseRetryWrapper.class);

  int currentCloseAttempts = 0;
  int numberOfClosesRequired;
  boolean throwExceptionsOfFailedClose;

  public MockFsDataOutputStreamCloseRetryWrapper(FSDataOutputStream wrapMe,
      int numberOfClosesRequired, boolean throwExceptionsOfFailedClose)
      throws IOException {
    super(wrapMe.getWrappedStream(), null);

    this.numberOfClosesRequired = numberOfClosesRequired;
    this.throwExceptionsOfFailedClose = throwExceptionsOfFailedClose;

  }

  public MockFsDataOutputStreamCloseRetryWrapper(OutputStream out,
      Statistics stats) throws IOException {
    super(out, stats);

  }

  @Override
  public void close() throws IOException {
    currentCloseAttempts++;
    logger.info("Attempting to Close: '" + currentCloseAttempts + "' of '" + numberOfClosesRequired + "'");
    if (currentCloseAttempts > numberOfClosesRequired || numberOfClosesRequired == 0) {
      logger.info("closing file");
      super.close();
    } else {
      if (throwExceptionsOfFailedClose) {
        logger.info("no closed and throwing exception");
        throw new IOException("MockIOException");
      } else {
        logger.info("no closed and doing nothing");
      }
    }
  }

  public int getCurrentCloseAttempts() {
    return currentCloseAttempts;
  }
}
