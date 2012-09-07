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
package org.apache.flume.channel.file;


/**
 * Pointer to an Event on disk. This is represented in memory
 * as a long. As such there are methods to convert from this
 * object to a long and from a long to this object.
 */
class FlumeEventPointer {
  private final int fileID;
  private final int offset;
  FlumeEventPointer(int fileID, int offset) {
    this.fileID = fileID;
    this.offset = offset;
    /*
     * Log files used to have a header, now metadata is in
     * a separate file so data starts at offset 0.
     */
    if(offset < 0) {
      throw new IllegalArgumentException("offset = " + offset + "(" +
          Integer.toHexString(offset) + ")" + ", fileID = " + fileID
            + "(" + Integer.toHexString(fileID) + ")");
    }
  }
  int getFileID() {
    return fileID;
  }
  int getOffset() {
    return offset;
  }
  public long toLong() {
    long result = fileID;
    result = (long)fileID << 32;
    result += (long)offset;
    return result;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + fileID;
    result = prime * result + offset;
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FlumeEventPointer other = (FlumeEventPointer) obj;
    if (fileID != other.fileID) {
      return false;
    }
    if (offset != other.offset) {
      return false;
    }
    return true;
  }
  @Override
  public String toString() {
    return "FlumeEventPointer [fileID=" + fileID + ", offset=" + offset + "]";
  }
  public static FlumeEventPointer fromLong(long value) {
    int fileID = (int)(value >>> 32);
    int offset = (int)value;
    return new FlumeEventPointer(fileID, offset);
  }
}
