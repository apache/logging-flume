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
package org.apache.flume.conf.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to suppress SpotBugs warnings in Flume artifacts.
 *
 * <p>SpotBugs recognizes any annotation whose simple name is {@code SuppressFBWarnings}, so this
 * type lets us drop the dependency on the LGPL-licensed {@code spotbugs-annotations} artifact.</p>
 *
 * <p>This type is not exported via JPMS. Do <strong>not</strong> use in third-party modules.</p>
 */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.PARAMETER})
public @interface SuppressFBWarnings {

    /** The set of SpotBugs warnings to suppress. */
    String[] value() default {};
}
