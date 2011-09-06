package org.apache.flume;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.source.EventDrivenSourceRunner;
import org.apache.flume.source.PollableSourceRunner;

abstract public class SourceRunner implements LifecycleAware {

  public static SourceRunner forSource(Source source) {
    SourceRunner runner = null;

    if (source instanceof PollableSource) {
      runner = new PollableSourceRunner();
      ((PollableSourceRunner) runner).setSource((PollableSource) source);
    } else if (source instanceof EventDrivenSource) {
      runner = new EventDrivenSourceRunner();
      ((EventDrivenSourceRunner) runner).setSource((EventDrivenSource) source);
    } else {
      throw new IllegalArgumentException("No known runner type for source "
          + source);
    }

    return runner;
  }

}
