#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

param(
    [Parameter(Position=0, Mandatory=$true)]
    [ValidateSet('help','agent','avro-client','version')] $command,
    [Parameter()] [string] $conf,
    [Parameter()] [Alias('C')] [string] $classPath,
    [Parameter()] [Alias('property')] [string] $javaProperties,
    [Parameter()] [Alias('d')] [switch] $dryrun,
    [Parameter()] [Alias('plugins-path')] [string] $pluginsPath,

    [Parameter()] [Alias('f',"conf-file")] [string] $confFile,
    [Parameter()] [Alias('n')] [string] $name,

    [Parameter()] [string] $rpcProps,
    [Parameter()] [Alias('H',"host")] [string] $avroHost,
    [Parameter()] [Alias('p',"port")] [string] $avroPort,
    [Parameter()] [Alias("dirname")]  [string] $avroDirName,
    [Parameter()] [Alias("filename")] [string] $avroFileName,
    [Parameter()] [Alias('R',"headerFile")] [string] $avroHeaderFile
)

# when invoked from cmd shell, array arguments are treated as array with one string value
# so we accept classpath and javaProperty as string argument and split explicitly into array

if ($classPath -ne "") {
   $classPathArr = $classPath.Split(";")
} else {
   $classPathArr = @()
}

if($javaProperties -ne "") {
   $javaPropertiesArr = $javaProperties.Split(";")  # properties may have embedded comma, so using ; as delim
} else {
   $javaPropertiesArr = @()
}

# FUNCTIONS
Function PrintUsage {
    Write-Host "
Usage: flume-ng <command> [options]...

commands:
  help                  display this help text
  agent                 run a Flume agent
  avro-client           run an avro Flume client
  version               show Flume version info

global options:
  -conf  <conf>                           use configs in <conf> directory
  -classpath,-C  ""value1;value2; ..""    append to the classpath
  -property ""name1=val;name2=val; ..""   sets a JDK system property value
  -dryrun,-d                              do not actually start Flume (test)
  -plugins-path ""dir1;dir2""             semi-colon-separated list of plugins.d directories. See the
                                          plugins.d section in the user guide for more details.
                                          Default: \$FLUME_HOME/plugins.d

agent options:
  -conf-file,-f  <file>                specify a config file (required)
  -name,-n  <name>                     the name of this agent (required)

avro-client options:
  -rpcProps <file>           RPC client properties file with server connection params
  -host,-H  <host>           hostname to which events will be sent (required)
  -port,-p  <port>           port of the avro source (required)
  -dirname <dir>             directory to stream to avro source
  -filename  <file>          text file to stream to avro source [default: std input]
  -headerFile,-R <file>      headerFile containing headers as key/value pairs on each new line

  Either --rpcProps or both --host and --port must be specified.

Note that if <conf> directory is specified, then it is always included first
in the classpath."
}

Function EnumerateJars($path) {
   return Get-ChildItem $path -Filter *.jar | ? { "$_" -notmatch "slf4j-(api|log4j12).*jar" } | % { $_.FullName}
}

Function GetHadoopHome {
    if($env:HADOOP_PREFIX) {
        $hadoopBin = "$env:HADOOP_PREFIX\bin;"
    } elseif ($env:HADOOP_HOME) {
        $hadoopBin = "$env:HADOOP_HOME\bin;"
    }

    #Searches for hadoop.cmd in the HADOOP_HOME, current directory and path
    [String[]] $hadoopPaths = ("$hadoopBin;.;$env:PATH").Split(";") |
                                 ? { "$_" -ne "" -and (Test-Path $_ )} |
                                 ? { Test-Path ( Join-Path $_ "hadoop.cmd" ) }
    if($hadoopPaths -ne $Null ) {
       $binDir = $hadoopPaths[0] + "\.."
       return  Resolve-Path $binDir
    }
    Write-Host "WARN: HADOOP_PREFIX or HADOOP_HOME not found"
    return $Null
}

Function GetHbaseHome() {
    if($env:HBASE_HOME) {
        return $env:HBASE_HOME
    }

    #Searches for hbase.cmd in the HBASE_HOME, current directory and path
    [String[]] $hbasePaths = (".;$env:PATH").Split(";") |
                               ? { "$_" -ne "" -and (Test-Path $_) } |
                               ? { Test-Path (Join-Path $_ "hbase.cmd") }

    if($hbasePaths -ne $Null) {
        return $hbasePaths[0]
    }
    Write-Host "WARN: HBASE_HOME not found"
    return $Null
}

Function GetHiveHome() {
    if($env:HIVE_HOME) {
        return $env:HIVE_HOME
    }

    #Searches for hive.cmd in the HIVE_HOME, current directory and path
    [String[]] $hivePaths = (".;$env:PATH").Split(";") |
                               ? { "$_" -ne "" -and (Test-Path $_) } |
                               ? { Test-Path (Join-Path $_ "hive.cmd") }

    if($hivePaths -ne $Null) {
        return $hivePaths[0]
    }
    Write-Host "WARN: HIVE_HOME not found"
    return $Null
}

Function GetJavaLibraryPath ($cmd, $flumeLibDir) {
    $flumeCoreJar = EnumerateJars( $flumeLibDir ) | ? { $_ -match "flume-ng-core.*jar" }
    $output = & "$cmd" -classpath $flumeCoreJar org.apache.flume.tools.GetJavaProperty java.library.path

    # look for the line that has the desired property value
    if( $output -match "(?m)^java\.library\.path=(.*)$" ) {
      return $Matches[1].split(";") | % { $_ -replace "\\$" , ""}  # trim trailing \ char
    }
    return @();
}


Function GetClassPath ($cmd) {
    $output = & "$cmd" "classpath"
	return $output;
}

Function GetJavaPath {
    if ($env:JAVA_HOME) {
        return "$env:JAVA_HOME\bin\java.exe" }
    Write-Host "WARN: JAVA_HOME not set"
    return '"' + (Resolve-Path "java.exe").Path + '"'
}


function runFlume($javaClassPath, $javaLibraryPath, $javaOptions, $class, $javaProcessArgumentList)  {
  [string]$javaPath = GetJavaPath
  [string]$fullJavaCommand = "-classpath $javaClassPath -Djava.library.path=$javaLibraryPath $javaOptions $class $javaProcessArgumentList"
  if ($dryrun) {
    Write-Host 'Dry run mode enabled (will not actually initiate startup)'
    Write-Host "$javaPath $fullJavaCommand"
  } else {
    Write-Host "
  Running FLUME $command :
    class: $class
    arguments: $javaProcessArgumentList
    "

    $ErrorActionPreference = "Continue"
    $x = Start-Process $javaPath -ArgumentList "$fullJavaCommand" -Wait -NoNewWindow
  }
}


# The script will terminate if any steps fail
$ErrorActionPreference = "Stop"

#SWITCH FOR THE DIFFERENT COMMANDS
[string] $javaProcessArgumentList
switch ($command)
{
  'help' {
    PrintUsage
    return }

  'agent' {
    if (!$Name) {
      PrintUsage
      Write-Host "ERROR: Name parameter missing"
      return
    }
    if (!$ConfFile) {
      PrintUsage
      Write-Host "ERROR: ConfFile parameter missing"
      return
    }
    $class='org.apache.flume.node.Application'
    $confFile = '"' + (Resolve-Path $confFile).Path + '"'
    $javaProcessArgumentList = "-n $name -f $confFile"  }

  'avro-client' {
    $class='org.apache.flume.client.avro.AvroCLIClient'
    if("$rpcProps" -eq "") {
      if (!$AvroHost) {
        PrintUsage
        Write-Host "ERROR: Avro Host parameter missing"
        return
      }
      if (!$AvroPort) {
        PrintUsage
        Write-Host "ERROR: Avro Port parameter missing"
        return
      }
      $javaProcessArgumentList = " -H $AvroHost -p $AvroPort"
    } else {
      $javaProcessArgumentList += " --rpcProps ""$rpcProps"""
    }

    if ($avroHeaderFile) {
      $avroHeaderFile = (Resolve-Path $avroHeaderFile).Path
      $javaProcessArgumentList += " -R $avroHeaderFile" }
    if ($avroFileName) {
      $avroFileName = (Resolve-Path $avroFileName).Path
      $javaProcessArgumentList += " -F $avroFileName" }
    if($avroDirName) {
      $avroDirName = (Resolve-Path $avroDirName).Path
      $javaProcessArgumentList += " --dirname ""$avroDirName""" } }

  'version' {
    $class='org.apache.flume.tools.VersionInfo'
    $javaProcessArgumentList = "" }

  default {
    PrintUsage
    Write-Host "ERROR: Invalid command '$command'"
    return }
}

$FlumeHome = $null
if($env:FLUME_HOME) {
    $FlumeHome = $env:FLUME_HOME
}  else {
    $ScriptPath =  Split-Path -Parent $MyInvocation.MyCommand.Path
    $FlumeHome =  Split-Path -Parent $ScriptPath
}

########### Source flume-env.ps1 ##############

# allow users to override the default env vars via conf\flume-env.ps1
if( "$conf" -eq "" ) {
    if( Test-path ("$FlumeHome\conf") ) {
      $conf = "$FlumeHome\conf"
      Write-Host "WARN: Config directory not set. Defaulting to $conf"
    }
}
if ( "$conf" -ne "" )  {
  Write-Host "Sourcing environment configuration script $conf\flume-env.ps1"
  if ( Test-path  "$conf\flume-env.ps1" ) {
     . "$conf\flume-env.ps1"
  }  else {
     Write-Host "WARN: Did not find $conf\flume-env.ps1"
  }
}  else {
  Write-Host "WARN: No configuration directory found! Use --conf <dir> to set."
}

########### Setup JAVA_OPTS ##############

[string]$javaOptions="$JAVA_OPTS"
foreach ($opt in $javaPropertiesArr) {
  $javaOptions = "$javaOptions -D$opt"
}

########### Setup Classpath ###############
# flume\conf ; flume_home\lib\* ; cmdline ; env.ps1 ; plugins.d ; hadoop.cpath ; hbase.cpath ; hive.cpath

[string]$javaClassPath='"' + $conf + '"'
[string]$flumeLibJars=""
[string]$flumeLibDir = Resolve-Path "$FlumeHome\lib"

# Add FlumeHome\lib\* to class path
$javaClassPath = "$javaClassPath;""$flumeLibDir\*"""
$flumeLibJars = "$flumeLibDir\*"""

# Add classpath from cmd line & FLUME_CLASSPATH in flume-env.ps1
if ( $FLUME_CLASSPATH )  {
  $classPathArr = $FLUME_CLASSPATH.Split(";")
}
foreach ($path in $classPathArr) {
  $fullPath = (Resolve-Path $path).Path
  $javaClassPath = "$javaClassPath;""$fullPath"""
}

$javaLibraryPath = ""

# Add plugins.d into classpath and libpath
if ("$pluginsPath" -eq "") {
   $pluginsPath = "$FlumeHome\plugins.d"
}

foreach($plugin in  $pluginsPath.Split(";") )  {
  if ( Test-path "$plugin" ) {
    $pluginTmp1 = (@(Get-ChildItem "$plugin\*\lib") -join "\*"";""")
    if( "$pluginTmp1" -ne "" ) {
      $javaClassPath="$javaClassPath;""" + $pluginTmp1 + "\*"";"
    }

    $pluginTmp2 = (@(Get-ChildItem "$plugin\*\libext") -join "\*"";""")
    if( "$pluginTmp2" -ne "" ) {
      $javaClassPath="$javaClassPath;""" + $pluginTmp2 + "\*"";"
    }

    $javaLibraryPathTmp = (@(Get-ChildItem "$plugin\*\native") -join "\*"";""")
    if( "$javaLibraryPathTmp" -ne "" )  {
       $javaLibraryPath=  "$javaLibraryPath""" + "$javaLibraryPathTmp" + "\*"";"
    }
  }
}

# Add Hadoop classpath &  java.library.path
$hadoopHome = GetHadoopHome
if("$hadoopHome" -ne "") {
  $hadoopCmd = "$hadoopHome\bin\hadoop.cmd"
  if( Test-Path $hadoopCmd ) {
    Write-Host "Including Hadoop libraries found in ($hadoopHome) for DFS access"
    $javaClassPath = "$javaClassPath;""$hadoopHome\conf""";
    foreach ($path in  GetClassPath $hadoopCmd) {
      $javaClassPath = "$javaClassPath;""$path"""
    }

    foreach ( $path in GetJavaLibraryPath $hadoopCmd  $flumeLibDir ) {
       $javaLibraryPath = "$javaLibraryPath""$path"";"
    }
  } else {
    Write-Host "WARN: $hadoopCmd not be found. Unable to include Hadoop's classpath & java.library.path"
  }
} else {
  Write-Host "WARN: HADOOP_PREFIX not set. Unable to include Hadoop's classpath & java.library.path"
}

# Add HBase classpath  & java.library.path
$hbaseHome = GetHbaseHome
if( "$hbaseHome" -ne "" ) {
    $hbaseCmd = "$hbaseHome\bin\hbase.cmd"
    if( Test-Path $hbaseCmd ) {
      Write-Host "Including HBase libraries found via ($hbaseHome) for HBase access"
      foreach ( $path in GetClassPath $hbaseCmd ) {
          $javaClassPath = "$javaClassPath;""$path"""
      }
      $javaClassPath = "$javaClassPath;""$hbaseHome\conf"""

      foreach ( $path in GetJavaLibraryPath $hbaseCmd  $flumeLibDir ) {
          $javaLibraryPath = "$javaLibraryPath""$path"";"
      }
    } else {
        Write-Host "WARN: $hbaseCmd not be found. Unable to include HBase classpath and java.library.path"
    }
}

# Add Hive classpath
$hiveHome = GetHiveHome
if( "$hiveHome" -ne "" ) {
    $hiveLib = "$hiveHome\lib"
    if( Test-Path $hiveLib ) {
      Write-Host "Including Hive libraries found via ($hiveHome) for Hive access"
      $javaClassPath = "$javaClassPath;""$hiveLib\*"""
    } else {
      Write-Host "WARN: $hiveLib not found. Unable to include Hive into classpath"
    }

    $hiveConf = "$hiveHome\conf"
    if( Test-Path $hiveConf ) {
      Write-Host "Including Hive conf dir ($hiveConf) in classpath for Hive access"
      $javaClassPath = "$javaClassPath;""$hiveConf"""
    } else {
      Write-Host "WARN: $hiveConf not found. Unable to include it into classpath"
    }

    $hcatLib = "$hiveHome\hcatalog\share\hcatalog"
    if( Test-Path $hcatLib ) {
    Write-Host "Including HCatalog libraries ($hcatLib) for Hive access"
      $javaClassPath = "$javaClassPath;""$hcatLib\*"""
    } else {
        Write-Host "WARN: $hcatLib not found. Unable to include HCatalog into classpath"
    }
}

runFlume $javaClassPath $javaLibraryPath $javaOptions $class $javaProcessArgumentList
