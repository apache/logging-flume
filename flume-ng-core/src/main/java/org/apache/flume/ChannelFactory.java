package org.apache.flume;

import java.util.Set;

public interface ChannelFactory {

  public boolean register(String name, Class<? extends Channel> channelClass);

  public boolean unregister(String name);

  public Channel create(String name) throws InstantiationException;

  public Set<String> getChannelNames();

}
