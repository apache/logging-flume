<!--
 Licensed to Cloudera, Inc. under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  Cloudera, Inc. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
<html><head>
<!--(c) Copyright (2009) Cloudera, Inc.    -->
<!-- Retro web 1.0 flume reporter display -->
<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="com.cloudera.flume.master.FlumeMaster"
%>

</head>
<body>
<h1> Flume's Config </h1>

<h2>Configure a node</h2>
<form method=post action="command.jsp"> 
Configure node:
<input type="text" name="node" /> <br/>
Source: 
<input type="text" name="source" /> <br/>
Sink:
<input type="text" name="sink" /> <br/>
<input type="submit"/>
</form>

<h2>Configure nodes</h2>
<form method=post action="fullspec.jsp"> 
<!-- TODO (jon) put current configuration in here -->
<textarea rows="20", cols="60" name="specification"> 
</textarea>
<input type="submit"/>
</form>


</body></html>
