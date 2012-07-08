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
<!-- Retro web 1.0 flume reporter display -->
<%@ page
	contentType="text/html; charset=UTF-8"
	import="com.cloudera.flume.master.FlumeMaster"
	import="com.cloudera.flume.conf.*"
%>
<jsp:useBean id="cmd" class="com.cloudera.flume.master.ConfigCommand" scope="request"/>
<jsp:setProperty name="cmd" property="*"/>
<html>
<link rel="stylesheet" type="text/css" href="/flume.css">
<meta HTTP-EQUIV="REFRESH" content="5;url=flumeconfig.jsp"/>

<body>
<h2>Command received</h2>
You entered<br>
Node: <%= cmd.getNode() %> <br>
Source: <%= cmd.getSource() %> <br>
Sink: <%= cmd.getSink() %> <br>
<p>Please wait for a few seconds, and we'll redirect you back to
<a href="flumemaster.jsp">the Master overview</a>.
</body>
</html>
<%
	FlumeMaster.getInstance().submit(cmd.toCommand());
%>
