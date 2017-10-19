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
package org.apache.flume.source.http;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

/**
 *
 */
class FlumeHttpServletRequestWrapper implements HttpServletRequest {

  private BufferedReader reader;

  String charset;

  public FlumeHttpServletRequestWrapper(String data, String charset)
      throws UnsupportedEncodingException {
    reader = new BufferedReader(new InputStreamReader(
            new ByteArrayInputStream(data.getBytes(charset)), charset));
    this.charset = charset;
  }

  public FlumeHttpServletRequestWrapper(String data) throws UnsupportedEncodingException {
    this(data, "UTF-8");
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
  public Enumeration<String> getHeaders(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Enumeration<String> getHeaderNames() {
    throw new UnsupportedOperationException("Not supported yet.");
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
  public Enumeration<String> getAttributeNames() {
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
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getParameter(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Enumeration<String> getParameterNames() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String[] getParameterValues(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<String,String[]> getParameterMap() {
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
    return reader;
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
  public Enumeration<Locale> getLocales() {
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

  @Override
  public AsyncContext getAsyncContext() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getContentLengthLong() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public DispatcherType getDispatcherType() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public ServletContext getServletContext() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isAsyncStarted() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isAsyncSupported() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AsyncContext startAsync() throws IllegalStateException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AsyncContext startAsync(ServletRequest arg0, ServletResponse arg1)
      throws IllegalStateException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean authenticate(HttpServletResponse arg0)
      throws IOException, ServletException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String changeSessionId() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Part getPart(String arg0) throws IOException, ServletException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Collection<Part> getParts() throws IOException, ServletException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void login(String arg0, String arg1) throws ServletException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void logout() throws ServletException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <T extends HttpUpgradeHandler> T upgrade(Class<T> arg0)
      throws IOException, ServletException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
