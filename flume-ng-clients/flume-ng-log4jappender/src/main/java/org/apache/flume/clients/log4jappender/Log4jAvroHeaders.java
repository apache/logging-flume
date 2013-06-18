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
package org.apache.flume.clients.log4jappender;

public enum Log4jAvroHeaders {
  OTHER("flume.client.log4j.logger.other"),
  LOGGER_NAME("flume.client.log4j.logger.name"),
  LOG_LEVEL("flume.client.log4j.log.level"),
  MESSAGE_ENCODING("flume.client.log4j.message.encoding"),
  TIMESTAMP("flume.client.log4j.timestamp"),
  AVRO_SCHEMA_LITERAL("flume.avro.schema.literal"),
  AVRO_SCHEMA_URL("flume.avro.schema.url");

  private String headerName;
  private Log4jAvroHeaders(String headerName){
    this.headerName = headerName;
  }

  public String getName(){
    return headerName;
  }

  public String toString(){
    return getName();
  }

  public static Log4jAvroHeaders getByName(String headerName){
    Log4jAvroHeaders hdrs = null;
    try{
      hdrs = Log4jAvroHeaders.valueOf(headerName.toLowerCase().trim());
    }
    catch(IllegalArgumentException e){
      hdrs = Log4jAvroHeaders.OTHER;
    }
    return hdrs;
  }

}