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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A message is a thin wrapper around some bytes to push out of a network link
 * for now. Later we will look at serialization formats. 
 * 
 * Headers are used to encapsulate protocol-specific information. For example:
 * multicast protocols might store the set of recipients in the headers. 
 * 
 * Headers are disabled right now as they're not used, and this is a 
 * half done version suitable for Flume. TODO(henry)
 * 
 */
public class Message {
  NodeId from;
  Map<String, List<String>> headers = new HashMap<String, List<String>>();
  ByteBuffer contents = ByteBuffer.allocate(0);
  
  public Message() {
    
  }
  
  /**
   * Returned a reference to the byte contents of the message
   */
  public byte[] getContents() {
    return contents.array();  
  }
  
  public Message(NodeId node, Map<String,List<String>> hdrs, byte[] contents) {
    from = node;
    headers = hdrs;
    this.contents = ByteBuffer.wrap(contents); 
  }
  
  public Message(DataInputStream in) throws IOException {
    deserialize(in);
  }
  
  /**
   * Write a serialized representation of the message to a DataOutputStream.
   */
  public void serialize(DataOutputStream out) throws IOException {
    out.writeUTF(from.toString());
    // We don't use headers at the moment
 /*   out.writeInt(headers.size());
    for (Entry<String,List<String>> e : headers.entrySet()) {
      out.writeUTF(e.getKey());
      out.writeInt(e.getValue().size());
      for (String s : e.getValue()) {
        out.writeUTF(s);
      }
    }*/
    out.writeInt(contents.limit());
    out.write(contents.array());
  }
  
  public void deserialize(DataInputStream in) throws IOException {
    from = new TCPNodeId(in.readUTF());
    // Headers are disabled
  /*  headers = new HashMap<String,List<String>>(in.readInt());
    for (int i=0;i<headers.size();++i) {
      String k = in.readUTF();
      List<String> l = new LinkedList<String>();
      for (int j=0; j<in.readInt();++j) {
        l.add(in.readUTF());
      }
      headers.put(k, l);
    }*/
    int size = in.readInt();
    contents = ByteBuffer.allocate(size);    
    int numRead = in.read(contents.array());  
    if (numRead != size) {
      throw new IOException("Expected to get " + size + " bytes but only read "
          + numRead);
    }
  }
}
