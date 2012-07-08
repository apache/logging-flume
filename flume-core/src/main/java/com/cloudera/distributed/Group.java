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
package com.cloudera.distributed;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** 
 * A Group is a collection of nodes. Although this just looks like a collection
 * now, we might eventually extend this to Views, with sequence numbers and
 * all sorts of goodness.
 */
public class Group {
  final Set<TCPNodeId> nodes = new HashSet<TCPNodeId>();
  
  public void addNode(TCPNodeId node) {
    nodes.add(node);
  }
  
  public void removeNode(TCPNodeId node) {
    nodes.remove(node);
  }
  
  public boolean contains(TCPNodeId node) {
    return nodes.contains(node);
  }
  
  public int getSize() {
    return nodes.size();    
  }
  
  public Set<TCPNodeId> getNodes() {
    return Collections.unmodifiableSet(nodes);
  }
}
