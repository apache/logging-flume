/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.flume.handlers.debug;

/**
 * This class contains the basic data elements of a ChokeDecorator and simple
 * operations to perform on that data.
 */
public class ChokeInfoData {
  private int max = Integer.MAX_VALUE;
  private int bucket = 0;
  private String chokeID;

  /**
   *This constructor sets the limit on the Choke.
   */
  public ChokeInfoData(int limit, String id) {
    max = limit * ChokeManager.timeQuanta;
    chokeID = id;
  }

  synchronized public void setMaxLimit(int limit) {
    max = limit * ChokeManager.timeQuanta;
  }

  synchronized public int getMaxLimit() {
    return max;
  }

  synchronized public void removeTokens(int numTokens) {
    bucket = bucket - numTokens;
  }

  synchronized public Boolean bucketCompare(int numTokens) {
    return (bucket >= numTokens);
  }

  synchronized public void printState() {
    System.out.println("Choke Information of " + chokeID + ": Max =" + max
        + " Bucket=" + bucket + "\n");
  }

  synchronized public void bucketFillup() {
    bucket = max;
  }

}
