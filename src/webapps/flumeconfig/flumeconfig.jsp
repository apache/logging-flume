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
<jsp:include page="menu.jsp" />

<h1> Flume Config </h1>

<h2>Configure a single node</h2>
<form method=post action="command.jsp">
<table>
	<tr>
		<td>Configure node:</td>
		<td>
			<select name="nodeChoice">
			<option value="">Choose from list</option>
				<% for (String s : FlumeMaster.getInstance().getKnownNodes()) {
				%> <option value="<%= s %>"><%= s %></option>
					<%
					}
					%>
			</select>
		</td>
	</tr>
	<tr>
		<td>
			or specify another node:
		</td>
		<td>
			<input type="text" name="node" size=128 />
		</td>
	</tr>
	<tr>
		<td>Source: </td>
		<td><input type="text" name="source" size=128/> </td>
	</tr>
	<tr>
		<td>Sink:</td>
		<td><input type="text" name="sink" size=128/> </td>
	</tr>
	<tr>
		<td>
			<input type="submit"/>
		</td>
	</tr>
	</form>
</table>

<h2>Configure multiple nodes</h2>
<form method=post action="fullspec.jsp">
<!-- TODO (jon) put current configuration in here -->
<textarea rows="10" cols="60" name="specification">
</textarea>
<input type="submit"/>
</form>


</body></html>
