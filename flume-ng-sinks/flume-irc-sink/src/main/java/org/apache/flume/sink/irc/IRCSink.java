/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.sink.irc;

import java.io.IOException;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventListener;
import org.schwering.irc.lib.IRCModeParser;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class IRCSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(IRCSink.class);

  private static final int DEFAULT_PORT = 6667;
  private static final String DEFAULT_SPLIT_CHARS = "\n";

  private static final String IRC_CHANNEL_PREFIX = "#";

  private IRCConnection connection = null;

  private String hostname;
  private Integer port;
  private String nick;
  private String password;
  private String user;
  private String name;
  private String chan;
  private Boolean splitLines;
  private String splitChars;
  
  private CounterGroup counterGroup;

  static public class IRCConnectionListener implements IRCEventListener {

    public void onRegistered() {
    }

    public void onDisconnected() {
      logger.error("IRC sink disconnected");
    }

    public void onError(String msg) {
      logger.error("IRC sink error: {}", msg);
    }

    public void onError(int num, String msg) {
      logger.error("IRC sink error: {} - {}", num, msg);
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

  public IRCSink() {
    counterGroup = new CounterGroup();
  }

  public void configure(Context context) {
    hostname = context.getString("hostname");
    String portStr = context.getString("port");
    nick = context.getString("nick");
    password = context.getString("password");
    user = context.getString("user");
    name = context.getString("name");
    chan = context.getString("chan");
    splitLines = context.getBoolean("splitlines", false);
    splitChars = context.getString("splitchars");

    if (portStr != null) {
      port = Integer.parseInt(portStr);
    } else {
      port = DEFAULT_PORT;
    }

    if (splitChars == null) {
      splitChars = DEFAULT_SPLIT_CHARS;
    }
    
    Preconditions.checkState(hostname != null, "No hostname specified");
    Preconditions.checkState(nick != null, "No nick specified");
    Preconditions.checkState(chan != null, "No chan specified");
  }

  private void createConnection() throws IOException {
    if (connection == null) {
      logger.debug(
          "Creating new connection to hostname:{} port:{}",
          hostname, port);
      connection = new IRCConnection(hostname, new int[] { port },
          password, nick, user, name);
      connection.addIRCEventListener(new IRCConnectionListener());
      connection.setEncoding("UTF-8");
      connection.setPong(true);
      connection.setDaemon(false);
      connection.setColors(false);
      connection.connect();
      connection.send("join " + IRC_CHANNEL_PREFIX + chan);
    }
  }

  private void destroyConnection() {
    if (connection != null) {
      logger.debug("Destroying connection to: {}:{}", hostname, port);
      connection.close();
    }

    connection = null;
  }

  @Override
  public void start() {
    logger.info("IRC sink starting");

    try {
      createConnection();
    } catch (Exception e) {
      logger.error("Unable to create irc client using hostname:"
          + hostname + " port:" + port + ". Exception follows.", e);

      /* Try to prevent leaking resources. */
      destroyConnection();

      /* FIXME: Mark ourselves as failed. */
      return;
    }

    super.start();

    logger.debug("IRC sink {} started", this.getName());
  }

  @Override
  public void stop() {
    logger.info("IRC sink {} stopping", this.getName());

    destroyConnection();

    super.stop();

    logger.debug("IRC sink {} stopped. Metrics:{}", this.getName(), counterGroup);
  }

  private void sendLine(Event event) {
    String body = new String(event.getBody());
    
    if (splitLines) {
      String[] lines = body.split(splitChars);
      for(String line: lines) {
        connection.doPrivmsg(IRC_CHANNEL_PREFIX + this.chan, line);
      }
    } else {
      connection.doPrivmsg(IRC_CHANNEL_PREFIX + this.chan, body);
    }
    
  }
  
  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();
      createConnection();

      Event event = channel.take();

      if (event == null) {
        counterGroup.incrementAndGet("event.empty");
        status = Status.BACKOFF;
      } else {
        sendLine(event);
        counterGroup.incrementAndGet("event.irc");
      }

      transaction.commit();

    } catch (ChannelException e) {
      transaction.rollback();
      logger.error(
          "Unable to get event from channel. Exception follows.", e);
      status = Status.BACKOFF;
    } catch (Exception e) {
      transaction.rollback();
      logger.error(
          "Unable to communicate with IRC server. Exception follows.",
          e);
      status = Status.BACKOFF;
      destroyConnection();
    } finally {
      transaction.close();
    }

    return status;
  }
}
