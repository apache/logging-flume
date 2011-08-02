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

package com.cloudera.flume.master;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.reporter.ReportEvent;

/** Master report tests */
public class TestMasterReport extends SetupMasterTestEnv {
  public static Logger LOG = Logger.getLogger(TestMasterReport.class);

  /** Verify that the report contains a particular set of attributes */
  @Test
  public void testReportContainsRequiredAttributes() {
    ReportEvent rpt = flumeMaster.getReport();

    Assert.assertNotNull(
        rpt.getStringMetric(FlumeMaster.REPORTKEY_HOSTNAME));
    Assert.assertNotNull(
        rpt.getLongMetric(FlumeMaster.REPORTKEY_NODES_REPORTING_COUNT));
        
    // log for visual inspection
    LOG.info(rpt);
  }
}
