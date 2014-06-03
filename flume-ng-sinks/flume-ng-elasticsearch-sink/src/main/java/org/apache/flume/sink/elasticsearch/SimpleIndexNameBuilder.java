/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.formatter.output.BucketPath;

public class SimpleIndexNameBuilder implements IndexNameBuilder {

  private String indexName;

  @Override
  public String getIndexName(Event event) {
    return BucketPath.escapeString(indexName, event.getHeaders());
  }

  @Override
  public String getIndexPrefix(Event event) {
    return BucketPath.escapeString(indexName, event.getHeaders());
  }

  @Override
  public void configure(Context context) {
    indexName = context.getString(ElasticSearchSinkConstants.INDEX_NAME);
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }
}
