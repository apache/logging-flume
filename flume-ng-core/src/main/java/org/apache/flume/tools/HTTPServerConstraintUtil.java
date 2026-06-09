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
package org.apache.flume.tools;

import org.eclipse.jetty.ee11.servlet.security.ConstraintMapping;
import org.eclipse.jetty.ee11.servlet.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.Constraint;

/**
 * Utility class to define constraints on Jetty HTTP servers
 */
public class HTTPServerConstraintUtil {

    private HTTPServerConstraintUtil() {}

    /**
     * Generate constraints for the Flume HTTP Source
     * @return ConstraintSecurityHandler for use with Jetty servlet
     */
    public static ConstraintSecurityHandler enforceConstraints() {
        // 1. Create a constraint that denies TRACE and OPTIONS access
        Constraint constraint = Constraint.from("Deny Methods", Constraint.Authorization.FORBIDDEN);

        // 2. Map the constraint to methods TRACE and OPTIONS on all paths
        ConstraintMapping traceMapping = new ConstraintMapping();
        traceMapping.setPathSpec("/*");
        traceMapping.setMethod("TRACE");
        traceMapping.setConstraint(constraint);

        ConstraintMapping optionsMapping = new ConstraintMapping();
        optionsMapping.setPathSpec("/*");
        optionsMapping.setMethod("OPTIONS");
        optionsMapping.setConstraint(constraint);

        // 3. Configure the ConstraintSecurityHandler
        ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
        securityHandler.setConstraintMappings(new ConstraintMapping[]{traceMapping, optionsMapping});

        return securityHandler;
    }
}
