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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.google.common.base.Preconditions;

/**
 * Gossip-based multicast. When given a group of peers to send to and a group
 * of peers who may be involved in gossip, choose a peer at random, exchange
 * digests of messages and send those that your peer does not have.
 * 
 * Termination conditions are flexible; typically time elapsed is appropriate. 
 * 
 * There are thousands of variations on gossip. This is the vanilla sort. 
 * 
 */
public class GossipMulticast implements Multicast<GossipMulticast.GossipMessage> {
  static final Logger LOG = LoggerFactory.getLogger(GossipMulticast.class);
  
  public GossipMulticast(Group group, TCPNodeId me) {
    this.group = group;
    this.node = me;
  }
  
  final Group group;
  final TCPNodeId node;
  GossipThread gossipThread = null;
  GossipServer gossipServer = null;
  final long MAX_AGE_MS = 60 * 1000; // Messages a minute old get aged off the queue
  volatile IOException lastException = null; // Set by threads to pass error conditions
  
  final Map<String,GossipMessage> digestMap = 
    new ConcurrentHashMap<String,GossipMessage>();
  
  // This is really used for managing aged messages
  final ConcurrentLinkedQueue<GossipMessage> msgQueue = 
    new ConcurrentLinkedQueue<GossipMessage>();
  
  // For the time being, we keep digests of all the messages we have ever seen
  // so that we never re-enqueue messages that we already saw but expired.
  // We might *receive* these messages more than once, but it would require
  // an extra step in the protocol to send a list of requested digests to
  // ensure we only get messages we've never seen. Instead, we just drop
  // those messages upon consultation with this set. 
  
  // We could also age these messages out after, say, 5*MAX_AGE_MS which 'should'
  // be the last time anyone sends the message. This would be easier with 
  // synchronized global clocks.
  final Set<String> seenMsgs = new HashSet<String>();
  
  /**
   * Starts both server and client threads 
   */
  public synchronized void start() throws IOException {
    Preconditions.checkState(gossipThread == null 
        && gossipServer == null);
    gossipThread = new GossipThread();
    gossipThread.start();
    gossipServer = new GossipServer();
    gossipServer.start();    
    try {
      if (!gossipThread.waitForStart()) {        
        throw new IOException("GossipThread did not start in time, last exception in multicast server was:", lastException);
      }
      if (!gossipServer.waitForStart()) {
        throw new IOException("GossipServer did not start in time, last exception in multicast server was:", lastException);
      }
    } catch (InterruptedException e) {
      throw new IOException("Multicast server couldn't start",e);
    }
  }
  
  /**
   * Interrupts both server and client threads and waits for them to exit   
   */
  public synchronized void stop() throws InterruptedException {
    LOG.info("Stopping gossip thread");
    gossipThread.shutdown();
    gossipServer.shutdown();
    gossipServer.join();
    gossipThread.join();
    gossipThread = null;
    gossipServer = null;
  }
  
  /**
   * A message type specialized to gossipping,   
   */
  public class GossipMessage extends Message {
    Group group = null;
    String digest = null;     
    long timestamp = 0;
    
    public GossipMessage(Group to, byte[] msg) {
      super(node, new HashMap<String,List<String>>(), msg);
      this.group = to;
      from = node;
    }       
    
    public GossipMessage(DataInputStream in) throws IOException {
      deserialize(in);
    }

    @Override
    public void serialize(DataOutputStream out) throws IOException {
      super.serialize(out);
      out.writeUTF(digest);
      out.writeInt(group.getSize());
      for (NodeId n : group.getNodes()) {
        out.writeUTF(n.toString());
      }
    }
    
    @Override
    public void deserialize(DataInputStream in) throws IOException {
      super.deserialize(in);      
      digest = in.readUTF();
      int groupsize = in.readInt();
      group = new Group();
      for (int i=0;i<groupsize;++i) {
        group.addNode(new TCPNodeId(in.readUTF()));
      }
    }
    
    void computeDigest() {
      MessageDigest algorithm;
      try {
        algorithm = MessageDigest.getInstance("MD5");
        algorithm.reset();
        algorithm.update(contents);
        digest = new String(algorithm.digest());        
      } catch (NoSuchAlgorithmException e) {       
        e.printStackTrace();
        this.digest = "";
      }      
    }
    
    public String getDigest() {
      if (digest == null) {
        computeDigest();
      }
      return digest;
      }
    }
        
  /**
   * This message contains a list of hashes of messages which can be
   * used to determine a list of messages to receive and send to peers.
   */
  class GossipDigestMessage extends Message {
    Set<String> digests = new HashSet<String>();
    Set<String> msgsIn = null;
    Set<String> msgsOut = null;
    
    public GossipDigestMessage() {
      // By default, build digest list from what we have
      digests = new HashSet<String>(msgQueue.size());
      for (GossipMessage gMsg : msgQueue) {
        digests.add(gMsg.getDigest());
      }
      from = node;
    }
    
    public GossipDigestMessage(DataInputStream in) throws IOException {
      deserialize(in);
    }
    
    @Override
    public void serialize(DataOutputStream out) throws IOException {
      super.serialize(out);
      out.writeInt(digests.size());
      for (String digest : digests) {
        out.writeUTF(digest);
      }
    }
    
    @Override
    public void deserialize(DataInputStream in) throws IOException {
      super.deserialize(in);
      int size = in.readInt();
      digests = new HashSet<String>(size);
      for (int i=0;i<size;++i) {                
        digests.add(in.readUTF());
      }            
    }
    
    void computeDiff() {
      msgsIn = new HashSet<String>(digestMap.keySet());      
      msgsIn.removeAll(digests);
      msgsOut = new HashSet<String>(digests);
      msgsOut.removeAll(digestMap.keySet());      
    }
    
    /**
     * Returns the set of messages that this peer has which the remote does not.
     * 
     */
    public Set<String> getMsgsIn() {
      return msgsIn;
    }
    
    /**
     * Returns the set of messages that the remote has which this peer does not.
     * 
     */
    public Set<String> getMsgsOut() {
      return msgsOut;
    }        
  }
      
  
  void enqueue(GossipMessage gMsg) {    
    gMsg.timestamp = System.currentTimeMillis();
    digestMap.put(gMsg.getDigest(), gMsg);
    seenMsgs.add(gMsg.getDigest());
    msgQueue.add(gMsg);
  }
  
  @Override
  public void sendToGroup(Group to, byte[] msg) {    
    GossipMessage gMsg = new GossipMessage(to, msg);
    LOG.debug(node.toString() + ": Enqueuing " + new String(msg));
    enqueue(gMsg);
  } 
  
  class GossipThread extends Thread {
    volatile boolean running = false;
    final CountDownLatch started = new CountDownLatch(1);
    
    public void shutdown() {
      running = false;      
    }
    
    /**
     * Returns false if thread did not succesfully start within 5s 
     */
    public boolean waitForStart() throws InterruptedException {
      return started.await(5000, TimeUnit.MILLISECONDS);
    }
    
    public void run() {
      Preconditions.checkState(running == false);
      
      List<TCPNodeId> nodes = new ArrayList<TCPNodeId>(group.getNodes());
      Random rand = new Random();
      int period = FlumeConfiguration.get().getMasterGossipPeriodMs();
      running = true;      
      started.countDown();
      // Every x seconds, wake up and pick a peer to send a digest to. 
      try {        
        while (running) {
          Thread.sleep(period);
          if (msgQueue.size() > 0 && nodes.size() > 0) {
            TCPNodeId n = nodes.get(rand.nextInt(nodes.size()));
            if (!node.toString().equals(n.toString())) {
              try {
                Socket sock = new Socket(n.getHost(), n.getPort());
                sock.setSoTimeout(5000);      
                DataOutputStream out = new DataOutputStream(sock.getOutputStream());
                DataInputStream in = new DataInputStream(sock.getInputStream());              
                doClientGossip(in, out);
                sock.close();
              } catch (IOException e) {
                // This error just gets logged so that failed peers don't break us
                LOG.error("IOException when gossiping with " + node.toString(), e);
              } 
            }
          }
          // While we're here, age off some of the queue
          long time = System.currentTimeMillis();
          ageMessages(time);
        }
      } catch (InterruptedException e) {
        LOG.error("GossipThread was interrupted!", e);
      }
    }
  }
  
  /**
   * Calls all attached message receivers to inform them of the new message
   */
  public void receiveMessage(GossipMessage msg) {
    for (MessageReceiver<GossipMessage> r : receivers) {
      r.receiveMessage(msg);
    }
  }

  /**
   * This thread waits for connections and then plays the secondary-partner
   * role in a gossip exchange.  
   * 
   * Currently this is single threaded which should be roughly ok, but should
   * be improved when a chance arises. TODO(henry)
   */
  class GossipServer extends Thread {
    volatile boolean running = false;
    final CountDownLatch started = new CountDownLatch(1);
    
    public void shutdown() {
      running = false;
    }
        
    /**
     * Returns false if thread did not succesfully start within 5s 
     */
    public boolean waitForStart() throws InterruptedException {
      return started.await(5000, TimeUnit.MILLISECONDS);
    }
    
    public void run() {      
      ServerSocket sock = null;
      try {
        sock = new ServerSocket(((TCPNodeId)node).getPort());
        sock.setReuseAddress(true);
        sock.setSoTimeout(2000);
      } catch (IOException e) {
        LOG.error("GossipServer couldn't start", e);
        lastException = e;
        return;
      }
      running = true;
      started.countDown();
      while (running) {
        Socket s = null;
        try {
          // Single threaded gossip server, wouldn't be hard to make multithreaded.          
          try {
            s = sock.accept(); // Will timeout
          } catch (SocketTimeoutException e) {
            continue;
          }
          s.setSoTimeout(5000);
          DataInputStream in = new DataInputStream(s.getInputStream());
          DataOutputStream out = new DataOutputStream(s.getOutputStream());
          doServerGossip(in, out);                 
        } catch (IOException e) {
          LOG.error("GossipServer saw error when gossiping with peer", e);           
        } finally {
          if (s !=null) {
            try {              
              s.close();
            } catch (IOException e) {
              LOG.warn("Failed to close connection, not an error", e);
            }
          }
        }
      }
      try {
        sock.close();
      } catch (IOException e) {
        LOG.warn("Failed to cleanly close server socket", e);
        lastException = e;
      }
    }
  }

  final List<MessageReceiver<GossipMessage>> receivers = 
    new LinkedList<MessageReceiver<GossipMessage>>();
  
  @Override
  public void registerReceiver(MessageReceiver<GossipMessage> receiver) {
    receivers.add(receiver);
  }

  /**
   * Remove messages from the queue that are too old
   */
  protected void ageMessages(long time) {
    while (!msgQueue.isEmpty()) {
      GossipMessage msg = msgQueue.peek();
      // Note: we are the only consumers of this queue (other threads add
      // but atomically), so we don't need to worry about msg being null
      // - as soon as someone else can take from this queue, we must
      // become worried.
      long delta = time - msg.timestamp;
      if (delta > MAX_AGE_MS) {
        LOG.info("Aging message " + new String(msg.getContents()));
        msgQueue.remove();
        synchronized (digestMap) {
          digestMap.remove(msg.getDigest());
        }
      } else {
        break;
      }
    }
  }

  /**
   * Play the part of the server in a gossip protocol. 
   */
  protected void doServerGossip(DataInputStream in,
      DataOutputStream out) throws IOException {
    GossipDigestMessage dMsg = new GossipDigestMessage(in);    
    // Between promising the new digests and sending them we don't want
    // the digestMap to get modified in such a way that a message we 
    // promised isn't there when we looked for it.
    // There are ways to circumvent this in the protocol (be able to send
    // 'null' messages, for example) but this locking regime is easy enough.          
    synchronized (digestMap) {      
      GossipDigestMessage myDigest = new GossipDigestMessage();
      dMsg.computeDiff();      
      myDigest.serialize(out);
      // What we could do here is take copies of everything in dMsg.getMsgsIn
      // and then release the lock, so that we don't hold the lock for a lot of IO
      for (int i=0;i<dMsg.getMsgsOut().size();++i) {
        GossipMessage gMsg = new GossipMessage(in);
        if (!seenMsgs.contains(gMsg.getDigest())) {
          enqueue(gMsg);
          receiveMessage(gMsg);
        }
      }
      for (String b : dMsg.getMsgsIn()) {
        digestMap.get(b).serialize(out);
      }
    }
  }

  /**
   * Play the part of the client (initiator) in a gossip protocol
   */
  protected int doClientGossip(DataInputStream in, DataOutputStream out)
      throws IOException {
    int count = 0;
    GossipDigestMessage dMsg = new GossipDigestMessage();  
    dMsg.serialize(out);
    dMsg.deserialize(in);
    dMsg.computeDiff();
    
    for (String b : dMsg.getMsgsIn()) {          
      digestMap.get(b).serialize(out);
    }
    for (int i=0;i<dMsg.getMsgsOut().size();++i) {
      GossipMessage gMsg = new GossipMessage(in);
      if (!seenMsgs.contains(gMsg.getDigest())) {
        ++count;
        enqueue(gMsg);           
        receiveMessage(gMsg);
      }          
    }    
    return count;
  } 
}  

