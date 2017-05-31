package org.apache.flume.sink.cassandra;

import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;

import java.util.Map;

/**
 * Created by roger.lu on 17/5/5.
 */
public interface CassandraEventSerializer extends Configurable, ConfigurableComponent {

  public Map<String, Object> getActions(byte[] payload);

}
