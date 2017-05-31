package org.apache.flume.sink.cassandra;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

import java.util.Map;

/**
 * Created by roger.lu on 17/5/10.
 */
public class JsonCassandraEventSerializer implements CassandraEventSerializer {


  @Override
  public Map<String, Object> getActions(byte[] payload) {

    String event = new String(payload, Charsets.UTF_8);
    return (Map<String, Object>)JSONObject.parse(event);

  }

  @Override
  public void configure(Context context) {

  }

  @Override
  public void configure(ComponentConfiguration componentConfiguration) {

  }
}
