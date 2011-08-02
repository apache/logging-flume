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
<html>
<link rel="stylesheet" type="text/css" href="/flume.css">
<head>
<!--(c) Copyright (2009) Cloudera, Inc.  All rights reserved. -->
<!-- Retro web 1.0 flume configuration master display -->
<title>Flume Master</title>

<%@ page
	contentType="text/html; charset=UTF-8"
	import="javax.servlet.*"
	import="javax.servlet.http.*"
	import="java.io.*"
	import="java.util.*"
	import="java.text.DecimalFormat"
	import="com.cloudera.flume.master.FlumeMaster"
	import="com.cloudera.flume.conf.*"
%>

</head>
<body>
<jsp:include page="menu.jsp" />
<h1>Flume Master</h1>
<jsp:include page="version.jsp" />

<h3>ServerID: <%= FlumeConfiguration.get().getMasterServerId()%></h3>
<h3>Servers <% for (String s :
FlumeConfiguration.get().getMasterServers().split(",")) { %>
<a href="http://<%= s %>:<%= FlumeConfiguration.get().getMasterHttpPort() %>"><%= s %></a>
<% } %>
</h3>

<hr>

<% FlumeMaster.getInstance().reportHtml(out); %>

</body></html>
