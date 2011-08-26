package org.apache.flume.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flume.EventSource;
import org.apache.flume.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultSourceFactory implements SourceFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultSourceFactory.class);

  public Map<String, Class<? extends EventSource>> sourceRegistry;

  public DefaultSourceFactory() {
    sourceRegistry = new HashMap<String, Class<? extends EventSource>>();
  }

  @Override
  public boolean register(String name, Class<? extends EventSource> sourceClass) {
    logger.info("Register source name:{} class:{}", name, sourceClass);

    if (sourceRegistry.containsKey(name)) {
      return false;
    }

    sourceRegistry.put(name, sourceClass);
    return true;
  }

  @Override
  public boolean unregister(String name) {
    logger.info("Unregister source class:{}", name);

    return sourceRegistry.remove(name) != null;
  }

  @Override
  public Set<String> getSourceNames() {
    return sourceRegistry.keySet();
  }

  @Override
  public EventSource create(String name) throws InstantiationException {
    Preconditions.checkNotNull(name);

    logger.debug("Creating instance of source {}", name);

    /* FIXME: Is returning null really a good idea? Should we just panic? */
    if (!sourceRegistry.containsKey(name)) {
      return null;
    }

    EventSource source = null;

    try {
      source = sourceRegistry.get(name).newInstance();
    } catch (IllegalAccessException e) {
      throw new InstantiationException("Unable to create source " + name
          + " due to " + e.getMessage());
    }

    return source;
  }

  @Override
  public String toString() {
    return "{ sinkRegistry:" + sourceRegistry + " }";
  }

  public Map<String, Class<? extends EventSource>> getSourceRegistry() {
    return sourceRegistry;
  }

  public void setSourceRegistry(
      Map<String, Class<? extends EventSource>> sourceRegistry) {
    this.sourceRegistry = sourceRegistry;
  }

}
