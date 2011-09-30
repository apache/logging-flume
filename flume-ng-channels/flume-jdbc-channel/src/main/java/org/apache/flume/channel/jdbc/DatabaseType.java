package org.apache.flume.channel.jdbc;

public enum DatabaseType {
  /** All other databases */
  OTHER,

  /** Apache Derby */
  DERBY,

  /** MySQL */
  MYSQL,

  /** PostgreSQL */
  PGSQL,

  /** Oracle */
  ORACLE;
}
