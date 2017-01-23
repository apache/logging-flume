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


package org.apache.flume.sink.hive;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.Shell;
import org.apache.hive.hcatalog.streaming.QueryFailedException;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class TestUtil {

  private static final String txnMgr = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";

  /**
   * Set up the configuration so it will use the DbTxnManager, concurrency will be set to true,
   * and the JDBC configs will be set for putting the transaction and lock info in the embedded
   * metastore.
   * @param conf HiveConf to add these values to.
   */
  public static void setConfValues(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, txnMgr);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    conf.set("fs.raw.impl", RawFileSystem.class.getName());
  }

  public static void createDbAndTable(Driver driver, String databaseName,
                                      String tableName, List<String> partVals,
                                      String[] colNames, String[] colTypes,
                                      String[] partNames, String dbLocation)
          throws Exception {
    String dbUri = "raw://" + dbLocation;
    String tableLoc = dbUri + Path.SEPARATOR + tableName;

    runDDL(driver, "create database IF NOT EXISTS " + databaseName + " location '" + dbUri + "'");
    runDDL(driver, "use " + databaseName);
    String crtTbl = "create table " + tableName +
            " ( " +  getTableColumnsStr(colNames,colTypes) + " )" +
            getPartitionStmtStr(partNames) +
            " clustered by ( " + colNames[0] + " )" +
            " into 10 buckets " +
            " stored as orc " +
            " location '" + tableLoc +  "'" +
            " TBLPROPERTIES ('transactional'='true')";

    runDDL(driver, crtTbl);
    System.out.println("crtTbl = " + crtTbl);
    if (partNames != null && partNames.length != 0) {
      String addPart = "alter table " + tableName + " add partition ( " +
              getTablePartsStr2(partNames, partVals) + " )";
      runDDL(driver, addPart);
    }
  }

  private static String getPartitionStmtStr(String[] partNames) {
    if ( partNames == null || partNames.length == 0) {
      return "";
    }
    return " partitioned by (" + getTablePartsStr(partNames) + " )";
  }

  // delete db and all tables in it
  public static void dropDB(HiveConf conf, String databaseName)
      throws HiveException, MetaException {
    IMetaStoreClient client = new HiveMetaStoreClient(conf);
    try {
      for (String table : client.listTableNamesByFilter(databaseName, "", (short)-1)) {
        client.dropTable(databaseName, table, true, true);
      }
      client.dropDatabase(databaseName);
    } catch (TException e) {
      client.close();
    }
  }

  private static String getTableColumnsStr(String[] colNames, String[] colTypes) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < colNames.length; ++i) {
      sb.append(colNames[i] + " " + colTypes[i]);
      if (i < colNames.length - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  // converts partNames into "partName1 string, partName2 string"
  private static String getTablePartsStr(String[] partNames) {
    if (partNames == null || partNames.length == 0) {
      return "";
    }
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < partNames.length; ++i) {
      sb.append(partNames[i] + " string");
      if (i < partNames.length - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  // converts partNames,partVals into "partName1=val1, partName2=val2"
  private static String getTablePartsStr2(String[] partNames, List<String> partVals) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < partVals.size(); ++i) {
      sb.append(partNames[i] + " = '" + partVals.get(i) + "'");
      if (i < partVals.size() - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  public static ArrayList<String> listRecordsInTable(Driver driver, String dbName, String tblName)
      throws CommandNeedRetryException, IOException {
    driver.run("select * from " + dbName + "." + tblName);
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    return res;
  }

  public static ArrayList<String> listRecordsInPartition(Driver driver, String dbName,
                                                         String tblName, String continent,
                                                         String country)
      throws CommandNeedRetryException, IOException {
    driver.run("select * from " + dbName + "." + tblName + " where continent='"
            + continent + "' and country='" + country + "'");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    return res;
  }

  public static class RawFileSystem extends RawLocalFileSystem {
    private static final URI NAME;

    static {
      try {
        NAME = new URI("raw:///");
      } catch (URISyntaxException se) {
        throw new IllegalArgumentException("bad uri", se);
      }
    }

    @Override
    public URI getUri() {
      return NAME;
    }

    static String execCommand(File f, String... cmd) throws IOException {
      String[] args = new String[cmd.length + 1];
      System.arraycopy(cmd, 0, args, 0, cmd.length);
      args[cmd.length] = f.getCanonicalPath();
      String output = Shell.execCommand(args);
      return output;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      File file = pathToFile(path);
      if (!file.exists()) {
        throw new FileNotFoundException("Can't find " + path);
      }
      // get close enough
      short mod = 0;
      if (file.canRead()) {
        mod |= 0444;
      }
      if (file.canWrite()) {
        mod |= 0200;
      }
      if (file.canExecute()) {
        mod |= 0111;
      }
      ShimLoader.getHadoopShims();
      return new FileStatus(file.length(), file.isDirectory(), 1, 1024,
              file.lastModified(), file.lastModified(),
              FsPermission.createImmutable(mod), "owen", "users", path);
    }
  }

  private static boolean runDDL(Driver driver, String sql) throws QueryFailedException {
    int retryCount = 1; // # of times to retry if first attempt fails
    for (int attempt = 0; attempt <= retryCount; ++attempt) {
      try {
        driver.run(sql);
        return true;
      } catch (CommandNeedRetryException e) {
        if (attempt == retryCount) {
          throw new QueryFailedException(sql, e);
        }
        continue;
      }
    } // for
    return false;
  }

}
