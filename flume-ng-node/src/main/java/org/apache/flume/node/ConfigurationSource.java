/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import java.io.InputStream;
import java.util.Locale;

/**
 * Interface for retrieving configuration data.
 */
public interface ConfigurationSource {

    String PROPERTIES = "properties";
    String JSON = "json";
    String YAML = "yaml";
    String YML = "yml";
    String XML = "xml";

    /**
     * Returns the InputStream if it hasn't already been processed.
     * @return The InputStream or null.
     */
    InputStream getInputStream();

    /**
     * Returns the URI string.
     * @return The string URI.
     */
    String getUri();

    /**
     * Determine if the configuration data source has been modified since it was last checked.
     * @return true if the data was modified.
     */
    default boolean isModified() {
        return false;
    }

    /**
     * Return the "file" extension for the specified uri.
     * @return The file extension.
     */
    String getExtension();

    /**
     * Returns the extension of the last segment of a path. Dots in directory names are ignored.
     * @param path The path, which may be null.
     * @return The lower case file extension, or "properties" if the file has no extension.
     */
    static String getExtension(String path) {
        if (path == null) {
            return PROPERTIES;
        }
        String fileName = path.substring(path.lastIndexOf('/') + 1);
        int index = fileName.lastIndexOf('.');
        // A leading dot denotes a hidden file, not an extension.
        if (index <= 0 || index == fileName.length() - 1) {
            return PROPERTIES;
        }
        return fileName.substring(index + 1).toLowerCase(Locale.ROOT);
    }
}
