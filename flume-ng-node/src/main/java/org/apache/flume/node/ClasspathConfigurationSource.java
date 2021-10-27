/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import java.io.InputStream;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.conf.ConfigurationException;

public class ClasspathConfigurationSource implements ConfigurationSource {

  private final String path;
  private final URI uri;

  public ClasspathConfigurationSource(URI uri) {
    this.uri = uri;
    if (StringUtils.isNotEmpty(uri.getPath())) {
      // classpath:///filename && classpath:/filename
      this.path = uri.getPath().substring(1);
    } else if (StringUtils.isNotEmpty(uri.getAuthority())) {
      // classpath://filename
      this.path = uri.getAuthority();
    } else if (StringUtils.isNotEmpty(uri.getSchemeSpecificPart())) {
      // classpath:filename
      this.path = uri.getSchemeSpecificPart();
    } else {
      throw new ConfigurationException("Invalid uri: " + uri);
    }
  }

  @Override
  public InputStream getInputStream() {
    return this.getClass().getClassLoader().getResourceAsStream(path);
  }

  @Override
  public String getUri() {
    return this.uri.toString();
  }

  @Override
  public String getExtension() {
    int length = uri.getPath().indexOf(".");
    if (length <= 1) {
      return PROPERTIES;
    }
    return uri.getPath().substring(length + 1);
  }

  @Override
  public String toString() {
    return "{ classpath: " + path + "}";
  }

}
