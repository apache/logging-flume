package org.apache.flume;

import java.util.Set;

public interface SourceFactory {

  public boolean register(String sourceName,
      Class<? extends Source> sourceClass);

  public boolean unregister(String sourceName);

  public Source create(String sourceName) throws InstantiationException;

  public Set<String> getSourceNames();

}
