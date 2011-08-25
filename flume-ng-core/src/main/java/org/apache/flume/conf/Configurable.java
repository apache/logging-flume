package org.apache.flume.conf;

import org.apache.flume.Context;

public interface Configurable {

  public void configure(Context context);

}
