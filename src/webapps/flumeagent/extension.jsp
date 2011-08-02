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
<!-- Copyright (c) 2010 Cloudera, Inc.  All rights reserved. -->
<!-- Retro web 1.0 flume Agent configuration display -->
<title>Flume Node: Sources and Sinks</title>
<%@ page
	contentType="text/html; charset=UTF-8"
	import="javax.servlet.*"
	import="javax.servlet.http.*"
	import="java.io.*"
	import="java.util.*"
	import="java.text.DecimalFormat"
	import="com.cloudera.flume.conf.FlumeBuilder"
%>
<link rel="stylesheet" type="text/css" href="/flume.css" />

</head>
<body>
<jsp:include page="menu_agent.jsp" />

<h1>Extensions - Sink/Source/Decorator</h1>

<h2>Available Sinks</h2>
<table>

<%
  // treeset so that the list is sorted
  for (String n: new TreeSet<String>(FlumeBuilder.getSinkNames())) {
%>
    <tr><td><%= n %></td></tr>
<%
  }
%>

</table>

<h2>Available Sources</h2>
<table>

<%
  // treeset so that the list is sorted
  for (String n: new TreeSet<String>(FlumeBuilder.getSourceNames())) {
%>
    <tr><td><%= n %></td></tr>
<%
  }
%>

</table>

<h2>Available Decorators</h2>
<table>

<%
  // treeset so that the list is sorted
  for (String n: new TreeSet<String>(FlumeBuilder.getDecoratorNames())) {
%>
    <tr><td><%= n %></td></tr>
<%
  }
%>

</table>

</body></html>
