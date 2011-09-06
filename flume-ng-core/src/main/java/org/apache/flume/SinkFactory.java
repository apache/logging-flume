package org.apache.flume;

import java.util.Set;

public interface SinkFactory {

  public boolean register(String name, Class<? extends Sink> sinkClass);

  public boolean unregister(String name);

  public Sink create(String name) throws InstantiationException;

  public Set<String> getSinkNames();

}
