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
package org.apache.flume.configfilter.env;

import aQute.bnd.annotation.Resolution;
import aQute.bnd.annotation.spi.ServiceProvider;
import java.util.Map;
import org.apache.flume.configfilter.AbstractConfigFilter;
import org.apache.flume.configfilter.ConfigFilter;
import org.apache.flume.configfilter.ConfigFilterProvider;

public class EnvironmentVariableConfigFilter extends AbstractConfigFilter {

    @Override
    public String filter(String key) {
        return System.getenv(key);
    }

    @Override
    public void initializeWithConfiguration(Map<String, String> configuration) {
        // NO-OP
    }

    /**
     * Provides the {@code env} configuration filter type.
     */
    @ServiceProvider(value = ConfigFilterProvider.class, resolution = Resolution.OPTIONAL)
    public static final class Provider implements ConfigFilterProvider {

        @Override
        public String getName() {
            return "env";
        }

        @Override
        public Class<? extends ConfigFilter> getComponentClass() {
            return EnvironmentVariableConfigFilter.class;
        }
    }
}
