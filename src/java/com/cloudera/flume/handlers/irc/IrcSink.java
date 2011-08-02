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

import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventListener;
import org.schwering.irc.lib.IRCModeParser;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.google.common.base.Preconditions;

/**
 * This simple sink dumps data to irc.
 */
public class IrcSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(IrcSink.class);

  private IRCConnection conn;

  String host;
  int port;
  String nick;
  String pass;
  String user;
  String name;
  String chan;
  boolean ssl; // later iteration

  public IrcSink(String host, int port, String nick, String pass, String user,
      String name, String chan, boolean ssl) {
    this.host = host;
    this.port = port;
    this.nick = nick;
    this.pass = pass;
    this.user = user;
    this.name = name;
    this.chan = chan;
    this.ssl = ssl;
  }

  public IrcSink(String host, int port, String nick, String chan) {
    this(host, port, nick, null, null, null, chan, false);
  }

  public void open() throws IOException {

    conn = new IRCConnection(host, new int[] { port }, pass, nick, user, name);
    conn.addIRCEventListener(new Listener());
    conn.setEncoding("UTF-8");
    conn.setPong(true);
    conn.setDaemon(false);
    conn.setColors(false);
    conn.connect();
    conn.send("join " + chan);
  }

  /**
   * TODO (jon) figure out which ones this has to pay attention to!
   */
  static public class Listener implements IRCEventListener {

    public void onRegistered() {
    }

    public void onDisconnected() {
    }

    public void onError(String msg) {
    }

    public void onError(int num, String msg) {
    }

    public void onInvite(String chan, IRCUser u, String nickPass) {
    }

    public void onJoin(String chan, IRCUser u) {
    }

    public void onKick(String chan, IRCUser u, String nickPass, String msg) {
    }

    public void onMode(IRCUser u, String nickPass, String mode) {
    }

    public void onMode(String chan, IRCUser u, IRCModeParser mp) {
    }

    public void onNick(IRCUser u, String nickNew) {
    }

    public void onNotice(String target, IRCUser u, String msg) {
    }

    public void onPart(String chan, IRCUser u, String msg) {
    }

    public void onPrivmsg(String chan, IRCUser u, String msg) {
    }

    public void onQuit(IRCUser u, String msg) {
    }

    public void onReply(int num, String value, String msg) {
    }

    public void onTopic(String chan, IRCUser u, String topic) {
    }

    public void onPing(String p) {
    }

    public void unknown(String a, String b, String c, String d) {
    }
  }

  public void close() throws IOException {
    conn.close();
  }

  @Override
  public void append(Event e) throws IOException {
    conn.doPrivmsg(this.chan, e.toString());
    super.append(e);
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length == 4,
            "usage: ircSink(host,port,nick,chan)");

        String host = argv[0];
        int port = Integer.parseInt(argv[1]);
        String nick = argv[2];
        String chan = argv[3];

        EventSink snk = new IrcSink(host, port, nick, chan);
        return snk;
      }
    };
  }
}
