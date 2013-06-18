/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.solr.morphline;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletInputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

class FlumeHttpServletRequestWrapper implements HttpServletRequest {

  private ServletInputStream stream;
  private String charset;
  
  public FlumeHttpServletRequestWrapper(final byte[] data) {
    stream = new ServletInputStream() {
      private final InputStream in = new ByteArrayInputStream(data);      
      @Override
      public int read() throws IOException {
        return in.read();
      }
    };
  }

  @Override
  public String getAuthType() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Cookie[] getCookies() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getDateHeader(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getHeader(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Enumeration getHeaders(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Enumeration getHeaderNames() {
    return Collections.enumeration(Collections.EMPTY_LIST);
  }

  @Override
  public int getIntHeader(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getMethod() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getPathInfo() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getPathTranslated() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getContextPath() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getQueryString() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getRemoteUser() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isUserInRole(String role) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Principal getUserPrincipal() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getRequestedSessionId() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getRequestURI() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public StringBuffer getRequestURL() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getServletPath() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public HttpSession getSession(boolean create) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public HttpSession getSession() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isRequestedSessionIdValid() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isRequestedSessionIdFromCookie() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isRequestedSessionIdFromURL() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isRequestedSessionIdFromUrl() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object getAttribute(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Enumeration getAttributeNames() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getCharacterEncoding() {
    return charset;
  }

  @Override
  public void setCharacterEncoding(String env) throws UnsupportedEncodingException {
    this.charset = env;
  }

  @Override
  public int getContentLength() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getContentType() {
    return null;
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    return stream;
  }

  @Override
  public String getParameter(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Enumeration getParameterNames() {
    return Collections.enumeration(Collections.EMPTY_LIST);
  }

  @Override
  public String[] getParameterValues(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map getParameterMap() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getProtocol() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getScheme() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getServerName() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getServerPort() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BufferedReader getReader() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getRemoteAddr() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getRemoteHost() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setAttribute(String name, Object o) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAttribute(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Locale getLocale() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Enumeration getLocales() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isSecure() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String path) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getRealPath(String path) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getRemotePort() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getLocalName() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getLocalAddr() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getLocalPort() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
