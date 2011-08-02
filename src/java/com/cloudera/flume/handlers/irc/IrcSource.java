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
package com.cloudera.flume.handlers.irc;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventListener;
import org.schwering.irc.lib.IRCModeParser;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.google.common.base.Preconditions;

/**
 * This logs in and listens to a irc server.
 * 
 * TODO (jon) share the connection to support multiple channels and to support
 * an IrcSink.
 */
public class IrcSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(IrcSource.class);

  private IRCConnection conn;

  String host;
  int port;
  String nick;
  String pass;
  String user;
  String name;
  String chan;
  boolean ssl; // later iteraton

  BlockingQueue<Event> q = new LinkedBlockingQueue<Event>();

  public IrcSource(String host, int port, String nick, String pass,
      String user, String name, String chan, boolean ssl) {
    this.host = host;
    this.port = port;
    this.nick = nick;
    this.pass = pass;
    this.user = user;
    this.name = name;
    this.chan = chan;
    this.ssl = ssl;
  }

  public IrcSource(String host, int port, String nick, String chan) {
    this(host, port, nick, null, null, null, chan, false);
  }

  @Override
  public void open() throws IOException {
    // TODO (jon) check if the nick usage was successful or not
    conn = new IRCConnection(host, new int[] { port }, pass, nick, user, name);
    conn.addIRCEventListener(new Listener());
    conn.setEncoding(FlumeConfiguration.get().getFlurkerEncoding());
    conn.setPong(true);
    conn.setDaemon(false);
    conn.setColors(false);
    conn.connect();

    conn.send("join " + chan);
    // TODO(jon) check if channel join was successful or not
  }

  @Override
  public void close() throws IOException {
    conn.close();
  }

  private void append(String s) {
    if (s == null) {
      LOG.error("null append!");
      return;
    }
    q.add(new EventImpl(s.getBytes()));
  }

  // Need to pick which call backs should be "sources" and which are "sinks".

  /**
   * Treats IRC events. The most of them are just printed.
   * 
   * TODO (jon) since we have fields, we can use store this as structured data.
   */
  public class Listener implements IRCEventListener {

    public void onRegistered() {
      append("Connected");
    }

    public void onDisconnected() {
      append("Disconnected");
    }

    public void onError(String msg) {
      append("Error: " + msg);
    }

    public void onError(int num, String msg) {
      append("Error #" + num + ": " + msg);
    }

    public void onInvite(String chan, IRCUser u, String nickPass) {
      append(chan + "> " + u.getNick() + " invites " + nickPass);
    }

    public void onJoin(String chan, IRCUser u) {
      append(chan + "> " + u.getNick() + " joins");
    }

    public void onKick(String chan, IRCUser u, String nickPass, String msg) {
      append(chan + "> " + u.getNick() + " kicks " + nickPass);
    }

    public void onMode(IRCUser u, String nickPass, String mode) {
      append("Mode: " + u.getNick() + " sets modes " + mode + " " + nickPass);
    }

    public void onMode(String chan, IRCUser u, IRCModeParser mp) {
      append(chan + "> " + u.getNick() + " sets mode: " + mp.getLine());
    }

    public void onNick(IRCUser u, String nickNew) {
      append("Nick: " + u.getNick() + " is now known as " + nickNew);
    }

    public void onNotice(String target, IRCUser u, String msg) {
      append(target + "> " + u.getNick() + " (notice): " + msg);
    }

    public void onPart(String chan, IRCUser u, String msg) {
      append(chan + "> " + u.getNick() + " parts");
    }

    public void onPrivmsg(String chan, IRCUser u, String msg) {
      append(chan + "> " + u.getNick() + ": " + msg);
    }

    public void onQuit(IRCUser u, String msg) {
      append("Quit: " + u.getNick());
    }

    public void onReply(int num, String value, String msg) {
      append("Reply #" + num + ": " + value + " " + msg);
    }

    public void onTopic(String chan, IRCUser u, String topic) {
      append(chan + "> " + u.getNick() + " changes topic into: " + topic);
    }

    public void onPing(String p) {

    }

    public void unknown(String a, String b, String c, String d) {
      append("UNKNOWN: " + a + " b " + c + " " + d);
    }
  }

  @Override
  public Event next() throws IOException {
    try {
      Event e =  q.take();
      updateEventProcessingStats(e);
      return e;
    } catch (InterruptedException e) {
      LOG.error("IrcSource interrupted", e);
      throw new IOException(e);
    }
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length == 4,
            "usage: ircSource(server, port, nick, chan)");
        String server = argv[0];
        int port = Integer.parseInt(argv[1]);
        String nick = argv[2];
        String chan = argv[3];
        return new IrcSource(server, port, nick, chan);
      }
    };
  }

  /**
   * A simple irc client using the source irc source interface.
   */
  public static void main(String[] argv) throws IOException {

    if (argv.length != 4) {
      System.err.println("Usage: IrcSource server port nick #channel");
      System.exit(-1);
    }

    String server = argv[0];
    int port = 0;
    try {
      port = Integer.parseInt(argv[1]);
    } catch (Exception e) {
      System.err.println("Problem parsing port number: " + argv[1]);
      System.exit(-1);
    }

    String nick = argv[2];
    String chan = argv[3];

    final IrcSource src = new IrcSource(server, port, nick, chan);

    new Thread() {
      public void run() {
        try {
          while (true) {
            System.out.println(src.next());
          }
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }.start();
    src.open();
  }
}
