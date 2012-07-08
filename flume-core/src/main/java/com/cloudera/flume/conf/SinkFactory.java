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

import static com.cloudera.util.ArrayUtils.toStrings;

import java.util.Set;

import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;

/**
 * This abstract class is used to generate Sink and Decorator instances from
 * strings and string arguments. Any numeric arguments are generally assumed to
 * be in decimal values.
 */
abstract public class SinkFactory {

  abstract public static class SinkBuilder {

    /**
     * The previous default was to build with string arguments. With function
     * arguments, we take objects instead. Internally this method is called and
     * to preserve backwards compatibility we translates objects into args.
     */
    public EventSink create(Context context, Object... args) {
      return build(context, toStrings(args));
    }

    /**
     * This is required for backwards compatibility and will eventually become
     * deprecated
     */
    @Deprecated
    abstract public EventSink build(Context context, String... argv);

    /**
     * We just about always want to use contexts so we can thread them through
     * the builder. This should only be used for tests.
     */
    @Deprecated
    public EventSink build(String... argv) {
      return build(new Context(), argv);
    }
  };

  abstract public static class SinkDecoBuilder {
    /**
     * The previous default was to build with string arguments. With function
     * arguments, we take objects instead. Internally this method is called and
     * to preserve backwards compatibility we translates objects into args.
     */
    public EventSinkDecorator<EventSink> create(Context context, Object... args) {
      return build(context, toStrings(args));
    }

    /**
     * This is required for backwards compatibility and will eventually become
     * deprecated
     */
    @Deprecated
    abstract public EventSinkDecorator<EventSink> build(Context context,
        String... argv);

    /**
     * We just about always want to use contexts so we can thread them through
     * the builder. This should only be used for tests.
     */
    @Deprecated
    public EventSinkDecorator<EventSink> build(String... argv) {
      return build(new Context(), argv);
    }
  }

  /**
   * Update to make object arguments instead of string arguments.
   */
  public EventSink createSink(Context context, String name, Object... args)
      throws FlumeSpecException {
    return getSink(context, name, toStrings(args));
  }

  @Deprecated
  abstract public EventSink getSink(Context context, String name,
      String... args) throws FlumeSpecException;

  /**
   * We just about always want to use contexts so we can thread them through the
   * builder. This should only be used for tests.
   */
  @Deprecated
  public EventSink getSink(String name, String... args)
      throws FlumeSpecException {
    return getSink(new Context(), name, args);
  }

  /**
   * Update to make object arguments instead of string arguments.
   */
  public EventSinkDecorator<EventSink> createDecorator(Context context,
      String name, Object... args) throws FlumeSpecException {
    return getDecorator(context, name, toStrings(args));
  }

  @Deprecated
  abstract public EventSinkDecorator<EventSink> getDecorator(Context context,
      String name, String... args) throws FlumeSpecException;

  /**
   * We just about always want to use contexts so we can thread them through the
   * builder. This should only be used for tests.
   */
  @Deprecated
  public EventSinkDecorator<EventSink> getDecorator(String name, String... args)
      throws FlumeSpecException {
    return getDecorator(new Context(), name, args);
  }

  /**
   * Returns the list of sinks that we can instantiate
   */
  abstract public Set<String> getSinkNames();

  /**
   * Returns the list of decorators that we can instantiate
   */
  abstract public Set<String> getDecoratorNames();
}
