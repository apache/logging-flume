@echo off

REM Licensed to Cloudera, Inc. under one
REM or more contributor license agreements.  See the NOTICE file
REM distributed with this work for additional information
REM regarding copyright ownership.  Cloudera, Inc. licenses this file
REM to you under the Apache License, Version 2.0 (the
REM "License"); you may not use this file except in compliance
REM with the License.  You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.


if "%OS%" == "Windows_NT" setlocal
rem ---------------------------------------------------------------------------
rem NT Service Install/Uninstall script for Flume
rem
rem Options
rem install                Install the service using Flume as service name.
rem                        Service is installed using default settings.
rem remove                 Remove the service from the System.
rem
rem name        (optional) If the second argument is present it is considered
rem                        to be new service name
rem
rem NOTE: This script requires:
rem   1) being run as administrator to have proper permissions
rem   2) have JAVA_HOME set properly
rem ---------------------------------------------------------------------------

REM ***** Check/Set FLUME_HOME *****
if NOT DEFINED FLUME_HOME set FLUME_HOME=%~dps0..

REM ***** Check/Set FLUME_CONF *****
if NOT DEFINED FLUME_CONF set FLUME_CONF=%FLUME_HOME%\conf

REM ***** Check/Set FLUME_MAIN *****
if NOT DEFINED FLUME_MAIN set FLUME_MAIN=com.cloudera.flume.agent.FlumeNodeDaemon

REM ***** Check JAVA_HOME is defined *****
if DEFINED JAVA_HOME goto java_home_defined_ok
echo The JAVA_HOME environment variable was not found...
echo Make sure the JAVA_HOME environment variable is correctly defined.
echo This environment variable is needed to run this program.
goto end
:java_home_defined_ok

REM ***** force JAVA HOME to be a shortened path name *****
echo JAVA_HOME is %JAVA_HOME%
rem call :expand "%JAVA_HOME%"


for %%i in ("%JAVA_HOME%") do call :expand %%i
goto :actually
:expand
set JAVA_HOME=%~dpfs1
echo changed to %JAVA_HOME%
goto :eof
:actually

REM *** Explanation
REM Convert "c:\program files\java\jre6" -> "c:\program~1\java\jre6"
REM %%i escapes the '%' so that we have a symbolic variable called %i.
REM create a list of items which ends up being the %JAVA_HOME% env variable.
REM feed it to a 'call' which takes a label and arguments
REM 'call' actually creates a new batch program instance, that jumps to the label argument which is :expand here.
REM the 'set' statement converts the argument 1 (which we feed JAVA_HOME into) to a short form, drive, path, filename. We can't apply this operator on regular variables, they need to be arugments. 
REM we goto :eof which exists the sub batch program context.
REM call falls out to the goto :actually
REM control moves to :actually and life continues.


REM ***** Make sure JAVA_HOME is actually set *****
if not "%JAVA_HOME%" == "" goto java_home_actually_set_ok
echo The JAVA_HOME environment variable is not defined properly.
echo This environment variable is needed to run this program.
goto end
:java_home_actually_set_ok

REM ***** SET JAVA_OPTS *****
set JAVA_OPTS=-ea;^
-Xdebug;^
-Xrunjdwp:transport=dt_socket,server=y,address=8888,suspend=n;^
-XX:TargetSurvivorRatio=90;^
-XX:+AggressiveOpts;^
-XX:+UseParNewGC;^
-XX:+UseConcMarkSweepGC;^
-XX:+CMSParallelRemarkEnabled;^
-XX:+HeapDumpOnOutOfMemoryError;^
-XX:SurvivorRatio=128;^
-XX:MaxTenuringThreshold=0

echo.
echo Setting JAVA_OPTS: %JAVA_OPTS%

REM ***** Check that flumenode.exe exists! *****
if exist "%FLUME_HOME%\bin\flumenode.exe" goto flume_home_ok
echo The flumenode.exe was not found.  If you have manually set FLUME_HOME is it set correctly?
goto end
:flume_home_ok


REM ***** Now we can setup all the PROCRUN variables!! *****

set EXECUTABLE=%FLUME_HOME%\bin\flumenode.exe
echo.
echo Using Executable: %EXECUTABLE%

REM ***** Set default Service name *****
set SERVICE_NAME=FlumeNode
set PR_DISPLAYNAME=FlumeNode

REM ***** Process batch parameters *****
if "%1" == "" goto displayUsage
if "%2" == "" goto setServiceName
set SERVICE_NAME=%2
set PR_DISPLAYNAME=%2
:setServiceName
if %1 == install goto doInstall
if %1 == remove goto doRemove
if %1 == uninstall goto doRemove
echo Unknown parameter "%1"

:displayUsage
echo.
echo Usage: flumenode-service.bat install/remove [service_name]
goto end

REM ***** Remove the service *****
:doRemove
"%EXECUTABLE%" //DS//%SERVICE_NAME%
echo.
echo The service '%SERVICE_NAME%' has been removed.
goto end

REM ***** Install the service *****
:doInstall
echo.
echo Installing the service '%SERVICE_NAME%' ...
echo.
echo Using FLUME_HOME:    %FLUME_HOME%
echo.
echo Using JAVA_HOME:        %JAVA_HOME%

REM ***** CLASSPATH library setting *****

REM ***** Ensure that any user defined CLASSPATH variables are not used on startup *****
set CLASSPATH=%FLUME_HOME%\build\classes

REM ***** For each jar in the FLUME_HOME lib directory call append to build the CLASSPATH variable.*****
for %%i in (%FLUME_HOME%\lib\*.jar) do call :append %%~fi
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1%2
goto :eof

:okClasspath

REM ***** Include the build\classes directory so it works in development *****
set FLUME_CLASSPATH=%CLASSPATH%;%FLUME_CONF%

REM ***** Set the FLUME_PARMS to include JAVA_OPTS plus storage-config ****
REM set FLUME_PARAMS=%JAVA_OPTS%;-Dflume;-Dstorage-config=%FLUME_CONF%;-Dflume-foreground=yes;-Dflume.home="%FLUME_HOME%"
set FLUME_PARAMS=%JAVA_OPTS%;-Dflume;-Dflume.root.logger=INFO,console;-Dflume.home=%FLUME_HOME%

echo.
echo Using FLUME_PARAMS:  %FLUME_PARAMS%


rem ***** Each PROCRUN command line option is prefixed with PR_ *****

set PR_DESCRIPTION=FlumeNodeService
set PR_INSTALL=%EXECUTABLE%
set PR_LOGPATH=%FLUME_HOME%\log
set PR_CLASSPATH=%FLUME_CLASSPATH%
set PR_STDOUTPUT=auto
set PR_STDERROR=auto
set PR_JVMMS=128
set PR_JVMMX=1024
set PR_STOPTIMEOUT=5

rem ***** Set the server jvm from JAVA_HOME *****
rem * server JRE
set PR_JVM=%JAVA_HOME%\bin\server\jvm.dll
if exist "%PR_JVM%" goto foundJvm
rem * client JRE (32-bit)
set PR_JVM=%JAVA_HOME%\bin\client\jvm.dll
if exist "%PR_JVM%" goto foundJvm
rem * server JDK 
set PR_JVM=%JAVA_HOME%\jre\bin\server\jvm.dll
if exist "%PR_JVM%" goto foundJvm
rem * client JDK (32-bit)
set PR_JVM=%JAVA_HOME%\jre\bin\client\jvm.dll
if exist "%PR_JVM%" goto foundJvm
set PR_JVM=auto
:foundJvm

echo.
echo Using JVM: %PR_JVM%

REM ***** Install the Service!!! *****
"%EXECUTABLE%" //IS//%SERVICE_NAME% --StartClass %FLUME_MAIN%  --StopClass %FLUME_MAIN%
if not errorlevel 1 goto installed
echo Failed installing '%SERVICE_NAME%' service.
goto end

REM ***** Update the service with some extra parameters *****
:installed
rem Clear the environment variables. They are not needed any more.
set PR_DISPLAYNAME=
set PR_DESCRIPTION=
set PR_INSTALL=
set PR_LOGPATH=
set PR_CLASSPATH=
set PR_JVM=
set PR_STDOUTPUT=
set PR_STDERROR=
set PR_JVMMS=
set PR_JVMMX=

REM ***** Update some more extra parameters ****
REM use jvm.dll to execute
REM call method windowsService("start") to start service
REM call method windowsService("stop") to stop service
REM Setup so that it automatically starts on bootup
"%EXECUTABLE%" //US//%SERVICE_NAME% --JvmOptions "%FLUME_PARAMS%" --StartMode jvm --StopMode jvm --StartMethod=windowsService --StartParams=start --StopMethod=windowsService --StopParams=stop --Startup auto

echo.
echo The service '%SERVICE_NAME%' has been installed.

:end
cd %CURRENT_DIR%
