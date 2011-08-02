/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.flume.conf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.ANTLRFileStream;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.collector.CollectorSink;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.BackOffFailOverSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.FanOutSink;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.master.availability.FailoverChainSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This wraps parsers and factories so that a simple string in the flume config
 * specification language can be used to generate complicated configurations.
 * 
 * NOTE: The language and the names of sources and sinks are case sensitive.
 * 
 * Here are some example specifications -- (formatted as 'code // comment'):
 * 
 * counter("foo") // Creates a counter sink with name foo
 * 
 * [ counter("foo") , thrift("host",1234) ] // FanOutSink with counter and a
 * thrift sink to host:1234
 * 
 * { intervalSampler(100) => counter("samplefoo") } // take every 100th event,
 * send to samplefoo counter
 * 
 * < thrift("host",1234) ? backup > // send to thrift at host:1234, and if fail
 * sends to backup.
 * 
 * let foo := counter("bar") in < { failsometimes => foo } ? foo > // let
 * variable substitution, both foo's after 'in' are the *same* instance.
 * 
 * And here's the fun part -- these are fully composable.
 * 
 * [ <thrift("host",1234) ? {rolling(1000) => writeaheadlog("/tmp/flume") } > ,
 * { intervalSampler(100) => grephisto("/specfile") } ]
 * 
 * // local report
 * 
 * NOTE: some of the names may change, the syntax may change, and it currently
 * doesn't do everything I would like, but it is a start.
 * 
 * TODO(jon) add usage to each Source/Sink/SinkDeco builder.
 */
public class FlumeBuilder {
  static final Logger LOG = LoggerFactory.getLogger(FlumeBuilder.class);

  static SourceFactory srcFactory = new SourceFactoryImpl();
  static SinkFactory sinkFactory = new SinkFactoryImpl();

  enum ASTNODE {
    DEC, HEX, OCT, STRING, BOOL, FLOAT, // literals
    SINK, SOURCE, // sink or source
    KWARG, // kwarg support
    MULTI, DECO, BACKUP, LET, ROLL, GEN, FAILCHAIN, // compound sinks
    NODE, // combination of sink and source
  };

  public static void setSourceFactory(SourceFactory srcFact) {
    Preconditions.checkNotNull(srcFact);
    srcFactory = srcFact;
  }

  public static void setSinkFactory(SinkFactory snkFact) {
    Preconditions.checkNotNull(snkFact);
    sinkFactory = snkFact;
  }

  /**
   * This hooks a particular string to the lexer. From there it creates a parser
   * that can be started from different entities. The lexer and language are
   * case sensitive.
   */
  static FlumeDeployParser getDeployParser(String s) {
    FlumeDeployLexer lexer = new FlumeDeployLexer(new ANTLRStringStream(s));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    return new FlumeDeployParser(tokens);
  }

  static CommonTree parse(String s) throws RecognitionException {
    return (CommonTree) getDeployParser(s).deflist().getTree();
  }

  static CommonTree parseHost(String s) throws RecognitionException {
    return (CommonTree) getDeployParser(s).host().getTree();
  }

  static CommonTree parseLiteral(String s) throws RecognitionException {
    return (CommonTree) getDeployParser(s).literal().getTree();
  }

  public static CommonTree parseSink(String s) throws RecognitionException {
    FlumeDeployParser parser = getDeployParser(s);
    CommonTree ast = (CommonTree) parser.sink().getTree();
    return ast;
  }

  public static CommonTree parseSource(String s) throws RecognitionException {
    return (CommonTree) getDeployParser(s).source().getTree();
  }

  /**
   * This is for reading and parsing out a full configuration from a file.
   */
  static CommonTree parseFile(String filename) throws IOException,
      RecognitionException {

    // Create a scanner and parser that reads from the input stream passed to us
    FlumeDeployLexer lexer = new FlumeDeployLexer(new ANTLRFileStream(filename));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FlumeDeployParser parser = new FlumeDeployParser(tokens);

    return (CommonTree) parser.deflist().getTree();
  }

  /**
   * This is for reading and parsing out a full configuration from a file.
   */
  static CommonTree parseNodeFile(String filename) throws IOException,
      RecognitionException {

    // Create a scanner and parser that reads from the input stream passed to us
    FlumeDeployLexer lexer = new FlumeDeployLexer(new ANTLRFileStream(filename));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FlumeDeployParser parser = new FlumeDeployParser(tokens);

    return (CommonTree) parser.connection().getTree();
  }

  public static Pair<EventSource, EventSink> buildNode(Context context, File f)
      throws IOException, RecognitionException, FlumeSpecException {
    CommonTree t = parseNodeFile(f.getCanonicalPath());
    if (t.getText() != "NODE") {
      throw new FlumeSpecException("fail, expected node but had "
          + t.toStringTree());
    }
    // String host = t.getChild(0).getText();
    CommonTree tsrc = (CommonTree) t.getChild(0);
    CommonTree tsnk = (CommonTree) t.getChild(1);

    return new Pair<EventSource, EventSink>(buildEventSource(context, tsrc),
        buildEventSink(context, tsnk, sinkFactory));
  }

  /**
   * This parses a aggregate configuration (name: src|snk; ...) and returns a
   * map from logical node name to a source sink pair. Context is required now
   * because a FlumeNode's PhysicalNode information may need to be passed in
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Pair<String, String>> parseConf(Context ctx,
      String s) throws FlumeSpecException {
    try {
      CommonTree node = parse(s);
      Map<String, Pair<String, String>> cfg = new HashMap<String, Pair<String, String>>();
      for (CommonTree t : (List<CommonTree>) node.getChildren()) {
        // -1 is magic EOF value
        if (t.getType() == -1) {
          break; // I am done.
        }

        if (t.getText() != "NODE") {
          throw new FlumeSpecException("fail, expected node but had "
              + t.toStringTree());
        }

        if (t.getChildCount() != 3) {
          throw new FlumeSpecException(
              "fail, node didn't wasn't (name,src,snk): " + t.toStringTree());
        }
        String host = t.getChild(0).getText();
        CommonTree tsrc = (CommonTree) t.getChild(1);
        CommonTree tsnk = (CommonTree) t.getChild(2);

        Pair<String, String> p = new Pair<String, String>(FlumeSpecGen
            .genEventSource(tsrc), FlumeSpecGen.genEventSink(tsnk));
        cfg.put(host, p);
      }
      return cfg;
    } catch (RecognitionException re) {
      LOG.error("Failure to parse and instantiate sink: '" + s + "'", re);
      throw new FlumeSpecException(re.toString());
    }

  }

  @SuppressWarnings("unchecked")
  public static Map<String, Pair<EventSource, EventSink>> build(
      Context context, String s) throws FlumeSpecException {
    try {
      CommonTree node = parse(s);
      Map<String, Pair<EventSource, EventSink>> cfg = new HashMap<String, Pair<EventSource, EventSink>>();
      for (CommonTree t : (List<CommonTree>) node.getChildren()) {
        // -1 is magic EOF value
        if (t.getType() == -1) {
          break; // I am done.
        }
        if (t.getText() != "NODE") {
          throw new FlumeSpecException("fail, expected node but had "
              + t.toStringTree());
        }
        String host = t.getChild(0).getText();
        CommonTree tsrc = (CommonTree) t.getChild(1);
        CommonTree tsnk = (CommonTree) t.getChild(2);

        Pair<EventSource, EventSink> p = new Pair<EventSource, EventSink>(
            buildEventSource(context, tsrc), buildEventSink(context, tsnk,
                sinkFactory));
        cfg.put(host, p);
      }
      return cfg;
    } catch (RecognitionException re) {
      LOG.error("Failure to parse and instantiate sink: '" + s + "'", re);
      throw new FlumeSpecException(re.toString());
    }

  }

  /**
   * Build a flume source from a flume config specification.
   * 
   * This should only throw FlumeSpecExceptions (No illegal arg exceptions
   * anymore)
   */
  public static EventSource buildSource(Context ctx, String s)
      throws FlumeSpecException {
    try {
      CommonTree srcTree = parseSource(s);
      return buildEventSource(ctx, srcTree);
    } catch (RecognitionException re) {
      LOG.debug("Failure to parse and instantiate sink: '" + s + "'", re);
      throw new FlumeSpecException(re.toString());
    } catch (NumberFormatException nfe) {
      LOG.debug("Failure to parse and instantiate sink: '" + s + "'", nfe);
      throw new FlumeSpecException(nfe.getMessage());
    } catch (IllegalArgumentException iae) {
      LOG.debug("Failure to parse and instantiate sink: '" + s + "'", iae);
      throw new FlumeSpecException(iae.getMessage());
    } catch (RuntimeRecognitionException re) {
      LOG.debug("Failure to parse and instantiate sink: '" + s + "'", re);
      throw new FlumeSpecException(re.getMessage());
    }
  }

  /**
   * Build a flume sink from a flume config specification. Sinks can be much
   * more complicated than sources.
   * 
   * This should only throw FlumeSpecExceptions (No illegal arg exceptions
   * anymore)
   */
  public static EventSink buildSink(Context context, String s)
      throws FlumeSpecException {
    try {
      CommonTree snkTree = parseSink(s);
      return buildEventSink(context, snkTree, sinkFactory);
    } catch (RecognitionException re) {
      LOG.debug("Failure to parse and instantiate sink: '" + s + "'", re);
      throw new FlumeSpecException(re.toString());
    } catch (NumberFormatException nfe) {
      LOG.debug("Failure to parse and instantiate sink: '" + s + "'", nfe);
      throw new FlumeSpecException(nfe.getMessage());
    } catch (IllegalArgumentException iae) {
      LOG.debug("Failure to parse and instantiate sink: '" + s + "'", iae);
      throw new FlumeSpecException(iae.getMessage());
    } catch (RuntimeRecognitionException re) {
      LOG.debug("Failure to parse and instantiate sink: '" + s + "'", re);
      throw new FlumeSpecException(re.getMessage());
    }
  }

  @Deprecated
  public static EventSink buildSink(String s) throws FlumeSpecException {
    return buildSink(new Context(), s);
  }

  /**
   * Formats the three pieces to a node configuration back into a single
   * parsable line
   */
  public static String toLine(String name, String src, String snk) {
    return name + " : " + src + " | " + snk + ";";
  }

  /**
   * All of factories expect all string arguments.
   * 
   * Numbers are expected to be in decimal format.
   * 
   * If it is of type KWARG, it returns null
   * 
   * TODO (jon) move to builders, or allow builders to take Integers/Booleans as
   * well as Strings
   */
  public static String buildSimpleArg(CommonTree t) throws FlumeSpecException {
    ASTNODE type = ASTNODE.valueOf(t.getText()); // convert to enum
    switch (type) {
    case HEX:
      String hex = t.getChild(0).getText();
      Preconditions.checkArgument(hex.startsWith("0x")); // bad parser if this
      // happens
      hex = hex.substring(2);
      Long i = Long.parseLong(hex, 16); // use base 16 radix
      return i.toString();
    case DEC:
      return t.getChild(0).getText();
    case BOOL:
      return t.getChild(0).getText();
    case OCT:
      String oct = t.getChild(0).getText();
      // bad parser if these happen
      Preconditions.checkArgument(oct.startsWith("0"));
      Preconditions.checkArgument(!oct.startsWith("0x"));
      Long i2 = Long.parseLong(oct, 8); // us base 16 radix
      return i2.toString();
    case FLOAT:
      return t.getChild(0).getText();
    case STRING:
      String str = t.getChild(0).getText();
      Preconditions.checkArgument(str.startsWith("\"") && str.endsWith("\""));
      str = str.substring(1, str.length() - 1);
      return StringEscapeUtils.unescapeJava(str);
    case KWARG:
      return null;
    default:
      throw new FlumeSpecException("Not a node of literal type: "
          + t.toStringTree());
    }
  }

  public static Pair<String, CommonTree> buildKWArg(CommonTree t) {
    ASTNODE type = ASTNODE.valueOf(t.getText()); // convert to enum
    Preconditions.checkArgument("KWARG".equals(type.toString()));
    String kw = t.getChild(0).getText();
    CommonTree arg = (CommonTree) t.getChild(1);
    return new Pair<String, CommonTree>(kw, arg);
  }

  /**
   * This method populates the id, argv,and kwargs into ctx needed to build
   * source/sink/deco name
   * 
   * @param t
   * @param ctx
   * @return
   * @throws FlumeSpecException
   */
  @SuppressWarnings("unchecked")
  static Pair<String, List<String>> handleArgs(CommonTree t, Context ctx)
      throws FlumeSpecException {
    List<CommonTree> children = (List<CommonTree>) new ArrayList<CommonTree>(t
        .getChildren());
    String sinkType = children.remove(0).getText();
    List<String> args = new ArrayList<String>();
    for (CommonTree tr : children) {
      String arg = buildSimpleArg(tr);
      if (arg != null) {
        args.add(arg);
      } else {
        Pair<String, CommonTree> kwarg = buildKWArg(tr);
        ctx.putValue(kwarg.getLeft(), buildSimpleArg(kwarg.getRight()));
      }
    }
    return new Pair<String, List<String>>(sinkType, args);
  }

  static EventSource buildEventSource(Context context, CommonTree t)
      throws FlumeSpecException {
    ASTNODE type = ASTNODE.valueOf(t.getText()); // convert to enum
    switch (type) {
    case SOURCE: {
      Context ctx = new Context(context);
      Pair<String, List<String>> idArgs = handleArgs(t, ctx);
      String sourceType = idArgs.getLeft();
      List<String> args = idArgs.getRight();

      EventSource src = srcFactory.getSource(ctx, sourceType, args
          .toArray(new String[0]));
      if (src == null) {
        throw new FlumeIdException("Invalid source: "
            + FlumeSpecGen.genEventSource(t));
      }
      return src;
    }
    default:
      throw new FlumeSpecException("bad parse tree! Expected source but got "
          + t.toStringTree());
    }
  }

  @SuppressWarnings("unchecked")
  static EventSink buildEventSink(Context context, CommonTree t,
      SinkFactory sinkFactory) throws FlumeSpecException {
    ASTNODE type = ASTNODE.valueOf(t.getText()); // convert to enum
    switch (type) {
    case SINK: {

      Context ctx = new Context(context);
      Pair<String, List<String>> idargs = handleArgs(t, ctx);
      String sinkType = idargs.getLeft();
      List<String> args = idargs.getRight();

      EventSink snk = sinkFactory.getSink(ctx, sinkType, args
          .toArray(new String[0]));

      if (snk == null) {
        throw new FlumeIdException("Invalid sink: "
            + FlumeSpecGen.genEventSink(t));
      }
      return snk;
    }
    case MULTI: {
      List<CommonTree> elems = (List<CommonTree>) t.getChildren();
      List<EventSink> snks = new ArrayList<EventSink>();
      try {
        for (CommonTree tr : elems) {
          EventSink s = buildEventSink(context, tr, sinkFactory);
          snks.add(s);
        }
        FanOutSink<EventSink> sink = new FanOutSink<EventSink>(snks);
        return sink;
      } catch (FlumeSpecException ife) {
        // TODO (jon) do something if there was an intermediate failure
        throw ife;
      }
    }
    case DECO: {
      List<CommonTree> decoNodes = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(decoNodes.size() <= 2,
          "Only supports one or no decorators per expression");
      // no decorators
      if (decoNodes.size() == 1) {
        CommonTree snkt = decoNodes.get(0);
        try {
          EventSink singleSnk = buildEventSink(context, snkt, sinkFactory);
          return singleSnk;
        } catch (FlumeSpecException ife) {
          throw ife;
        }
      }

      // single decorator
      CommonTree deco = decoNodes.get(0);
      CommonTree decoSnk = decoNodes.get(1);
      EventSinkDecorator<EventSink> decoSink = buildEventSinkDecorator(context,
          deco);
      try {
        EventSink dsnk = buildEventSink(context, decoSnk, sinkFactory);
        decoSink.setSink(dsnk);
        return decoSink;
      } catch (FlumeSpecException ife) {
        // TODO (jon) need to cleanup after mainsink if this failed.
        throw ife;
      }
    }

    case BACKUP: {
      List<CommonTree> backupNodes = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(backupNodes.size() == 2,
          "Only supports two retry nodes per failover expression");
      CommonTree main = backupNodes.get(0);
      CommonTree backup = backupNodes.get(1);
      try {
        EventSink mainSink = buildEventSink(context, main, sinkFactory);
        EventSink backupSink = buildEventSink(context, backup, sinkFactory);
        return new BackOffFailOverSink(mainSink, backupSink);
      } catch (FlumeSpecException ife) {
        LOG.error("Failed to build Failover sink", ife);
        throw ife;
      }
    }

    case LET: {
      List<CommonTree> letNodes = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(letNodes.size() == 3);
      String argName = letNodes.get(0).getText();
      CommonTree arg = letNodes.get(1);
      CommonTree body = letNodes.get(2);
      try {
        EventSink argSink = buildEventSink(context, arg, sinkFactory);

        // TODO (jon) This isn't exactly right. 'let' currently does
        // "substitution on parse", which means when there are multiple
        // instances of the let subexpression, it will get opened twice (which
        // is now illegal). This hack makes things work by relaxing open's
        // semantics so that multiple opens are ignored.

        // Another approach is to have "substitution on open". Let would work
        // differently than here. We would have LetDecorator that internally
        // keeps the subexpression and then body. The references to the
        // subexpression in the body would get replaced with a reference. On
        // open, the subexpression would be opened, and the body as well. As the
        // body is traversed, if the reference would not be opened, but would
        // substitute the (already) open subexpression in its place.

        EventSink argSinkRef = new EventSinkDecorator<EventSink>(argSink) {
          boolean open = false;

          @Override
          public void open() throws IOException, InterruptedException {
            if (open) {
              return; // Do nothing because already open
            }
            open = true;
            sink.open();
          }

          @Override
          public void append(Event e) throws IOException, InterruptedException {
            Preconditions.checkState(open);
            sink.append(e);
          }

          @Override
          public void close() throws IOException, InterruptedException {
            open = false;
            sink.close();
          }

        };

        // add arg to context

        LinkedSinkFactory linkedFactory = new LinkedSinkFactory(sinkFactory,
            argName, argSinkRef);
        EventSink bodySink = buildEventSink(context, body, linkedFactory);
        return bodySink;
      } catch (FlumeSpecException ife) {
        throw ife;
      }
    }

    case ROLL: {
      List<CommonTree> rollArgs = (List<CommonTree>) t.getChildren();
      try {
        Preconditions.checkArgument(rollArgs.size() == 2, "bad parse tree! "
            + t.toStringTree() + "roll only takes two arguments");
        CommonTree ctbody = rollArgs.get(0);
        Long period = Long.parseLong(buildSimpleArg(rollArgs.get(1)));
        String body = FlumeSpecGen.genEventSink(ctbody);
        // TODO (jon) replace the hard coded 250 with a parameterizable value
        RollSink roller = new RollSink(context, body, period, 250);
        return roller;
      } catch (IllegalArgumentException iae) {
        throw new FlumeSpecException(iae.getMessage());
      }

    }

    case GEN: {
      List<CommonTree> collArgs = (List<CommonTree>) t.getChildren();
      try {
        Preconditions.checkArgument(collArgs.size() >= 2, "bad parse tree! "
            + t.toStringTree() + " generator takes at least 2 arguments");
        String genType = collArgs.get(0).getText();
        if (!"collector".equals(genType)) {
          throw new FlumeSpecException(
              "currently only handle 'collector' gen sinks");
        }
        CommonTree ctbody = collArgs.remove(1); // remove subsink.
        String body = FlumeSpecGen.genEventSink(ctbody);

        Context ctx = new Context(context);
        Pair<String, List<String>> idArgs = handleArgs(t, ctx);
        String sourceType = idArgs.getLeft();
        List<String> args = idArgs.getRight();
        args.add(0, body);

        // TODO replace with Generator Sink lookup
        Preconditions.checkArgument("collector".equals(sourceType));
        SinkBuilder builder = CollectorSink.builder();

        return builder.build(ctx, args.toArray(new String[0]));

      } catch (IllegalArgumentException iae) {
        throw new FlumeSpecException(iae.getMessage());
      }

    }

    case FAILCHAIN: {
      // TODO (jon) This is no longer necessary with the substitution mechanisms
      // found in the translators
      List<CommonTree> rollArgs = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(rollArgs.size() >= 2);
      CommonTree ctbody = rollArgs.get(0);
      List<String> rargs = new ArrayList<String>(rollArgs.size() - 1);
      boolean first = true;
      for (CommonTree ct : rollArgs) {
        if (first) {
          first = false;
          continue;
        }
        // assumes this is a STRING
        rargs.add(buildSimpleArg(ct));
      }
      String body = FlumeSpecGen.genEventSink(ctbody);
      FlumeConfiguration conf = FlumeConfiguration.get();
      FailoverChainSink failchain = new FailoverChainSink(context, body, rargs,
          conf.getFailoverInitialBackoff(), conf.getFailoverMaxSingleBackoff());
      return failchain;
    }
      // TODO (jon) new feature: handle pattern match splitter
      // case MATCH:

    default:
      throw new FlumeSpecException("bad parse tree! expected sink but got "
          + t.toStringTree());
    }
  }

  @SuppressWarnings("unchecked")
  static EventSinkDecorator<EventSink> buildEventSinkDecorator(Context context,
      CommonTree t) throws FlumeSpecException {
    Context ctx = new Context(context);
    Pair<String, List<String>> idArgs = handleArgs(t, ctx);
    String sinkType = idArgs.getLeft();
    List<String> args = idArgs.getRight();
    EventSinkDecorator deco = sinkFactory.getDecorator(ctx, sinkType, args
        .toArray(new String[0]));
    if (deco == null) {
      throw new FlumeIdException("Invalid sink decorator: "
          + FlumeSpecGen.genEventSinkDecorator(t));
    }
    return deco;
  }

  /**
   * Returns an unmodifiable set containing the sinks this builder supports
   */
  public static Set<String> getSinkNames() {
    return sinkFactory.getSinkNames();
  }

  /**
   * Return an unmodifiable set containing the decorators this builder supports
   */
  public static Set<String> getDecoratorNames() {
    return sinkFactory.getDecoratorNames();
  }

  /**
   * Return an unmodifiable set containing the sources this builder supports
   */
  public static Set<String> getSourceNames() {
    return srcFactory.getSourceNames();
  }
}
