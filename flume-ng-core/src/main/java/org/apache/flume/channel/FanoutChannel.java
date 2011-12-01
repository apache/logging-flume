package org.apache.flume.channel;

import java.util.LinkedList;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FanoutChannel implements Channel {
  private final Logger logger = LoggerFactory
      .getLogger(FanoutChannel.class);

  /**
   * A wrapper transaction that does the operation on all channels.
   * Note that there's no two phase commit. If one of the channels
   * throws an exception, we still execute the operations for the 
   * rest. All the failed commits are rolled back at the end to 
   * maintain consistent transaction state.
   * Note that the currently commit and rollback are not throwing
   * exceptions * even if one of the underlying transactions fail.
   */
  public class wrapperTransaction implements Transaction {
    @Override
    public void begin() {
      boolean txnErrors = false;
      for (Channel ch : channelList) {
        try {
          ch.getTransaction().begin();
        } catch (ChannelException e) {
          txnErrors = true;
          logger.warn("Error in fanout commit" + ch.getName(), e);
        }
      }
      if (txnErrors == true) {
        throw new ChannelException("Errors in fanout begin");
      }
    }

    @Override
    public void commit() {
      boolean txnErrors = false;
      LinkedList<Transaction> failedTxnList = new LinkedList<Transaction> ();
      Transaction lastTxn = null;

      for (Channel ch : channelList) {
        try {
          lastTxn = ch.getTransaction();
          lastTxn.commit();
        } catch (ChannelException e) {
          txnErrors = true;
          logger.warn("Error in fanout commit" + ch.getName(), e);
          if (lastTxn != null) {
            failedTxnList.add(lastTxn);
          }
        }
      }
      if (txnErrors == true) {
        // TODO : Need some way to notify the caller about failed commits

        // rollback the transactions that we couldn't successfully commit
        for (Transaction tx : failedTxnList) {
          try {
            tx.rollback();
          } catch (ChannelException e) {
            // Ignore the exception during cleanup
          }
        }
      }
    }

    @Override
    public void rollback() {
      boolean txnErrors = false;

      for (Channel ch : channelList) {
        try {
        ch.getTransaction().rollback();
        } catch (ChannelException e) {
          txnErrors = true;
          logger.warn("Error in fanout rollback" + ch.getName(), e);
        }
      }
      if (txnErrors == true) {
        // TODO : Need some way to notify the caller about failed rollbacks
      }
    }

    @Override
    public void close() {
      boolean txnErrors = false;

      for (Channel ch : channelList) {
        try {
          ch.getTransaction().close();
        } catch (ChannelException e) {
          txnErrors = true;
          logger.warn("Error in fanout close" + ch.getName(), e);
        }
      }
      if (txnErrors == true) {
        throw new ChannelException("Errors in fanout close");
      }
    }
  }

  private LinkedList<Channel> channelList;
  private wrapperTransaction txn;

  public FanoutChannel() {
    channelList = new LinkedList<Channel>();
    txn = new wrapperTransaction();
  }

  public void addFanout(Channel ch) {
    channelList.add(ch);
  }

  @Override
  public void put(Event event) throws ChannelException {
    for (Channel ch : channelList) {
      ch.put(event);
    }
  }

  @Override
  public Event take() throws ChannelException {
    // fanout is really for sources, we don't want to support take
    throw new ChannelException("Can't take from fanout channel");
  }

  @Override
  public Transaction getTransaction() {
    return txn;
  }

  @Override
  public void shutdown() {
    for (Channel ch : channelList) {
      ch.shutdown();
    }
  }

  @Override
  public String getName() {
    return "SourceFanout";
  }

}
