package org.apache.flume.channel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flume.Channel;
import org.apache.flume.ChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultChannelFactory implements ChannelFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultChannelFactory.class);

  private Map<String, Class<? extends Channel>> channelRegistry;

  public DefaultChannelFactory() {
    channelRegistry = new HashMap<String, Class<? extends Channel>>();
  }

  @Override
  public boolean register(String name, Class<? extends Channel> channelClass) {
    logger.info("Register channel name:{} class:{}", name, channelClass);

    if (channelRegistry.containsKey(name)) {
      return false;
    }

    channelRegistry.put(name, channelClass);
    return true;
  }

  @Override
  public boolean unregister(String name) {
    logger.info("Unregister channel class:{}", name);

    return channelRegistry.remove(name) != null;
  }

  @Override
  public Set<String> getChannelNames() {
    return channelRegistry.keySet();
  }

  @Override
  public Channel create(String name) throws InstantiationException {
    Preconditions.checkNotNull(name);

    logger.debug("Creating instance of channel {}", name);

    if (!channelRegistry.containsKey(name)) {
      return null;
    }

    Channel channel = null;

    try {
      channel = channelRegistry.get(name).newInstance();
    } catch (IllegalAccessException e) {
      throw new InstantiationException("Unable to create channel " + name
          + " due to " + e.getMessage());
    }

    return channel;
  }

  @Override
  public String toString() {
    return "{ channelRegistry:" + channelRegistry + " }";
  }

  public Map<String, Class<? extends Channel>> getChannelRegistry() {
    return channelRegistry;
  }

  public void setChannelRegistry(
      Map<String, Class<? extends Channel>> channelRegistry) {
    this.channelRegistry = channelRegistry;
  }

}
