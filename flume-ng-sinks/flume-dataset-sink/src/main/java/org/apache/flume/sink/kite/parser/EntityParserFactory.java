/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.flume.sink.kite.parser;

import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Context;

import static org.apache.flume.sink.kite.DatasetSinkConstants.*;

public class EntityParserFactory {

  public EntityParser<GenericRecord> newParser(Schema datasetSchema, Context config) {
    EntityParser<GenericRecord> parser;

    String parserType = config.getString(CONFIG_ENTITY_PARSER,
        DEFAULT_ENTITY_PARSER);

    if (parserType.equals(AVRO_ENTITY_PARSER)) {
      parser = new AvroParser.Builder().build(datasetSchema, config);
    } else {

      Class<? extends EntityParser.Builder> builderClass;
      Class c;
      try {
        c = Class.forName(parserType);
      } catch (ClassNotFoundException ex) {
        throw new IllegalArgumentException("EntityParser.Builder class "
            + parserType + " not found. Must set " + CONFIG_ENTITY_PARSER
            + " to a class that implements EntityParser.Builder or to a builtin"
            + " parser: " + Arrays.toString(AVAILABLE_PARSERS), ex);
      }

      if (c != null && EntityParser.Builder.class.isAssignableFrom(c)) {
        builderClass = c;
      } else {
        throw new IllegalArgumentException("Class " + parserType + " does not"
            + " implement EntityParser.Builder. Must set "
            + CONFIG_ENTITY_PARSER + " to a class that extends"
            + " EntityParser.Builder or to a builtin parser: "
            + Arrays.toString(AVAILABLE_PARSERS));
      }

      EntityParser.Builder<GenericRecord> builder;
      try {
        builder = builderClass.newInstance();
      } catch (InstantiationException ex) {
        throw new IllegalArgumentException("Can't instantiate class "
            + parserType + ". Must set " + CONFIG_ENTITY_PARSER + " to a class"
            + " that extends EntityParser.Builder or to a builtin parser: "
            + Arrays.toString(AVAILABLE_PARSERS), ex);
      } catch (IllegalAccessException ex) {
        throw new IllegalArgumentException("Can't instantiate class "
            + parserType + ". Must set " + CONFIG_ENTITY_PARSER + " to a class"
            + " that extends EntityParser.Builder or to a builtin parser: "
            + Arrays.toString(AVAILABLE_PARSERS), ex);
      }

      parser = builder.build(datasetSchema, config);
    }

    return parser;
  }
}
