<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:exsl="http://exslt.org/common"
                version="1.0"
                exclude-result-prefixes="exsl">

<!--
   Licensed to Cloudera, Inc. under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   Cloudera, Inc. licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->


<xsl:import href="breadcrumbs.xsl"/>

<xsl:template name="user.head.content">
</xsl:template>

<xsl:template name="user.header.content">

    <div style="clear:both; margin-bottom: 4px" />
    <div align="center">
      <a href="index.html"><img src="images/home.png"
          alt="Documentation Home" /></a>
    </div>
    <span class="breadcrumbs">
    <xsl:call-template name="breadcrumbs"/>
    </span>

</xsl:template>
</xsl:stylesheet>
