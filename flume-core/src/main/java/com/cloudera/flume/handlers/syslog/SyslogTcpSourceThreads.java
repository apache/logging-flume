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
package com.cloudera.flume.handlers.syslog;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.text.EventExtractException;

/**
 * This source listens for multiple tcp-based syslog data streams. This works
 * for many concurrent connections, but may run into scaling problems.
 * 
 * syslog entries sent over tcp are simply delimited by '\n' characters and are
 * otherwise identical in format to udp-based syslog data.
 * 
 * TODO (jon) setup a limit on the number of threads, find out how to modify
 * filehandle/socket limits on windows/linux
 * 
 * TODO(jon) Either do an nio/asynchronous version,
 * 
 * TODO (jon) or make separate queues for each thread -- this may cause lock
 * contention
 */
public class SyslogTcpSourceThreads extends EventSource.Base {
  static final Logger LOG = LoggerFactory
      .getLogger(SyslogTcpSourceThreads.class);

  final public static int SYSLOG_TCP_PORT = 514;
  final int port;
  final LinkedBlockingQueue<Event> eventsQ = new LinkedBlockingQueue<Event>(100000);
  final List<ReaderThread> readers = Collections
      .synchronizedList(new ArrayList<ReaderThread>());
  final AtomicLong rejects = new AtomicLong();
  volatile boolean closed = true;

  public SyslogTcpSourceThreads(int port) {
    this.port = port;
  }

  public SyslogTcpSourceThreads() {
    this(SYSLOG_TCP_PORT); // this is syslog-ng's default tcp port.
  }

  // must synchronize this variable
  ServerThread svrthread;
  ServerSocket sock = null;
  Object sockLock = new Object();

  /**
   * This thread just waits to accept incoming connections and spawn a reader
   * thread.
   */
  class ServerThread extends Thread {
    final int port;

    ServerThread(int port) {
      this.port = port;
    }

    @Override
    public void run() {
      while (!closed) {
        ServerSocket mySock = null; // guarantee no NPE at accept
        synchronized (sockLock) {
          mySock = sock; // get a local reference to sock.
        }

        if (mySock == null || mySock.isClosed())
          return;

        try {
          Socket client = mySock.accept();
          new ReaderThread(client).start();
        } catch (IOException e) {
          if (!closed) {
            // could be IOException where we run out of file/socket handles.
            LOG.error("accept had a problem", e);
          }
          return;
        }
      }
    }
  };

  /**
   * This thread takes a accepted socket and pull data out until it is empty.
   */
  class ReaderThread extends Thread {
    Socket in;

    ReaderThread(Socket sock) {
      readers.add(this);
      this.in = sock;
    }

    @Override
    public void run() {
      try {
        // process this connection.
        DataInputStream dis = new DataInputStream(in.getInputStream());
        while (!closed) {
          try {
            Event e = SyslogWireExtractor.extractEvent(dis);
            if (e == null)
              break;
            eventsQ.put(e);
          } catch (EventExtractException ex) {
            rejects.incrementAndGet();
          }
        }
        // done.
        in.close();

      } catch (IOException e) {
        LOG.error("IOException with SyslogTcpSources", e);
      } catch (InterruptedException e1) {
        LOG.error("put into Queue interupted" + e1);
      } finally {
        if (in != null && in.isConnected()) {
          try {
            in.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
        readers.remove(this);
      }
    }
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing " + this);
    synchronized (sockLock) {
      closed = true;
      if (sock != null) {
        sock.close();
        sock = null;
      }
    }
    // wait for all readers to close (This is not robust!)
    if (readers.size() != 0) {
      List<ReaderThread> rs = new ArrayList<ReaderThread>(readers); // 
      for (ReaderThread r : rs) {
        try {
          r.join();
        } catch (InterruptedException e) {
          LOG.error("Reader threads interrupted, but we are closing", e);
        }
      }
    }
    try {
      if (svrthread != null) {
        svrthread.join();
        svrthread = null;
      }
    } catch (InterruptedException e) {
      LOG.error("Reader threads interrupted, but we are closing", e);
    }
  };

  @Override
  public Event next() throws IOException {
    Event e = null;
    try {
      while ((e = eventsQ.poll(1000, TimeUnit.MILLISECONDS)) == null && !closed) {
        // Do nothing, just checking variables if nothing has arrived.
      }

    } catch (InterruptedException e1) {
      LOG.error("Tcp source polling interrupted ", e1); // Fail by throwing exn
      throw new IOException(e1);
    }
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void open() throws IOException {
    LOG.info("Opening " + this);
    synchronized (sockLock) {
      if (!closed) {
        throw new IOException("Attempted to double open socket");
      }
      closed = false;

      if (sock == null) {
        // depending on number of connections, may need to increase backlog
        // value (automatic server socket argument, default is 50)
        try {
          sock = new ServerSocket(port);
          sock.setReuseAddress(true);
        } catch (IOException e) {
          throw new IOException("failed to create serversocket " + e);
        }
      }
    }
    svrthread = new ServerThread(port);
    svrthread.start();
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(Context ctx, String... argv) {
        int port = SYSLOG_TCP_PORT; // default udp port, need root permissions
        // for this.
        if (argv.length > 1) {
          throw new IllegalArgumentException("usage: syslogTcp([port no]) ");
        }

        if (argv.length == 1) {
          port = Integer.parseInt(argv[0]);
        }

        return new SyslogTcpSourceThreads(port);
      }

    };
  }

}
