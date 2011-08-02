package com.cloudera.flume.master;
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


import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.xml.bind.annotation.XmlRootElement;

import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.master.MasterAckManager;

@Path("/acks")
public class MasterAckManagerResource {
  private final MasterAckManager ackman;

  /**
   * Must be public for jersey to instantiate and process.
   */
  public MasterAckManagerResource() {
    this.ackman = FlumeMaster.getInstance().getAckMan();
  }

  /**
   * Wrapper class to provide XmlRootElement.
   */
  @XmlRootElement
  public static class Acks {
    public Set<String> acks;

    public Acks() {

    }

    public Acks(Set<String> cfgs) {
      this.acks = cfgs;
    }
  }

  /**
   * Dump the pending acks in json format.
   */
  @GET
  @Produces("application/json")
  public Acks getAcksJson() {
    return new Acks(ackman.getPending());
  }

}
