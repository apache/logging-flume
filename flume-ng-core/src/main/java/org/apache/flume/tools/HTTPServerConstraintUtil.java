/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.tools;

import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;

// Most of the code in this class is copied from HBASE-10473

/**
 * Utility class to define constraints on Jetty HTTP servers
 */

public class HTTPServerConstraintUtil {

  private HTTPServerConstraintUtil() {

  }

  /**
   * Generate constraints for the Flume HTTP Source
   * @return ConstraintSecurityHandler for use with Jetty servlet
   */
  public static ConstraintSecurityHandler enforceConstraints() {
    Constraint c = new Constraint();
    c.setAuthenticate(true);

    ConstraintMapping cmt = new ConstraintMapping();
    cmt.setConstraint(c);
    cmt.setMethod("TRACE");
    cmt.setPathSpec("/*");

    ConstraintMapping cmo = new ConstraintMapping();
    cmo.setConstraint(c);
    cmo.setMethod("OPTIONS");
    cmo.setPathSpec("/*");

    ConstraintSecurityHandler sh = new ConstraintSecurityHandler();
    sh.setConstraintMappings(new ConstraintMapping[]{cmt, cmo});

    return sh;
  }
}
