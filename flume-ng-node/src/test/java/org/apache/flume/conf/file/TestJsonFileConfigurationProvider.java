package org.apache.flume.conf.file;

import java.io.File;

import org.apache.flume.ChannelFactory;
import org.apache.flume.SinkFactory;
import org.apache.flume.SourceFactory;
import org.apache.flume.channel.DefaultChannelFactory;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.flume.sink.LoggerSink;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.DefaultSourceFactory;
import org.apache.flume.source.NetcatSource;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJsonFileConfigurationProvider {

  private static final File testFile = new File(
      TestJsonFileConfigurationProvider.class.getClassLoader()
          .getResource("flume-conf.json").getFile());
  private static final Logger logger = LoggerFactory
      .getLogger(TestJsonFileConfigurationProvider.class);

  private JsonFileConfigurationProvider provider;

  @Before
  public void setUp() {
    ChannelFactory channelFactory = new DefaultChannelFactory();
    SourceFactory sourceFactory = new DefaultSourceFactory();
    SinkFactory sinkFactory = new DefaultSinkFactory();

    channelFactory.register("memory", MemoryChannel.class);

    sourceFactory.register("seq", SequenceGeneratorSource.class);
    sourceFactory.register("netcat", NetcatSource.class);

    sinkFactory.register("null", NullSink.class);
    sinkFactory.register("logger", LoggerSink.class);

    provider = new JsonFileConfigurationProvider();

    provider.setChannelFactory(channelFactory);
    provider.setSourceFactory(sourceFactory);
    provider.setSinkFactory(sinkFactory);
  }

  @Test
  public void testLifecycle() throws InterruptedException {
    provider.setFile(testFile);

    provider.start();

    Thread.sleep(1000);
    provider.stop();

    logger.debug("provider:{}", provider);
  }

}
