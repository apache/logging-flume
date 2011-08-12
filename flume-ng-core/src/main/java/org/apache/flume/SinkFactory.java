package org.apache.flume;

import java.util.Set;

public interface SinkFactory {

  public boolean register(String name, Class<? extends EventSink> sinkClass);

  public boolean unregister(String name);

  public EventSink create(String name) throws InstantiationException;

  public Set<String> getSinkNames();

}
