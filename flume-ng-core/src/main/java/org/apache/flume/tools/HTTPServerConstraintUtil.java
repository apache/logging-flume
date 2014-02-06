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

import org.mortbay.jetty.security.Constraint;
import org.mortbay.jetty.security.ConstraintMapping;
import org.mortbay.jetty.security.SecurityHandler;
import org.mortbay.jetty.servlet.Context;

// Most of the code in this class is copied from HBASE-10473

/**
 * Utility class to impose constraints on Jetty HTTP servers
 */

public class HTTPServerConstraintUtil {

  private HTTPServerConstraintUtil() {

  }

  /**
   * Impose constraints on the {@linkplain org.mortbay.jetty.servlet.Context}
   * passed in.
   * @param ctx - {@linkplain org.mortbay.jetty.servlet.Context} to impose
   *            constraints on.
   */
  public static void enforceConstraints(Context ctx) {
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

    SecurityHandler sh = new SecurityHandler();
    sh.setConstraintMappings(new ConstraintMapping[]{cmt, cmo});
    ctx.addHandler(sh);
  }
}
