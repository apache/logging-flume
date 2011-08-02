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
<!-- Retro web 1.0 flume reporter display -->
<title>Flume Master: Direct Command</title>
<%@ page
	contentType="text/html; charset=UTF-8"
	import="javax.servlet.*"
	import="javax.servlet.http.*"
	import="java.io.*"
	import="java.util.*"
	import="java.text.DecimalFormat"
	import="com.cloudera.flume.master.FlumeMaster"
	import="com.cloudera.flume.master.commands.GenericCommand"
%>

<jsp:useBean id="rawcmd" class="com.cloudera.flume.master.commands.GenericCommand" scope="session"/>
<jsp:setProperty name="rawcmd" property="*"/>


</head>
<body>
<jsp:include page="menu.jsp" />

<h1> Flume Master: Direct Commands </h1>

This is a low level interface to the Flume Configuration Master's
internal command processor. Please consider using the FlumeShell instead!

<h2>Command</h2>
<form method="post" action="mastersubmit.jsp">
<!-- convert this to a drop down -->
Command:
<input type="text" name="cmd" /> <br/>
Arguments: <input type="text" name="args" /> <br/>
<input type="submit"/>
</form>


Some example commands:

command args
<ul>
<li>save: flumespecfile
<li>load: flumespecfile
<li>config: "node" "source" "sink"
<li>unconfig: "node"
<li>multiconfig: "node1: source | sink; node2 : source | sink; ... "
<li>startlogical: physicalnodename logicalnodename
</ul>

</body></html>
