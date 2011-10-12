package org.apache.flume.channel.jdbc.impl;

import javax.sql.DataSource;

public class MySQLSchemaHandler implements SchemaHandler {

  private final DataSource dataSource;

  protected MySQLSchemaHandler(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public boolean schemaExists() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void validateSchema() {
    // TODO Auto-generated method stub

  }

  @Override
  public void createSchemaObjects() {
    // TODO Auto-generated method stub

  }

}
