/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.flume.netty.filter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ipfilter.IpFilterRule;
import io.netty.handler.ipfilter.IpFilterRuleType;

/**
 * The Class PatternRule represents an IP filter rule using string patterns.
 * <br>
 * Rule Syntax:
 * <br>
 * <pre>
 * Rule ::= [n|i]:address          n stands for computer name, i for ip address
 * address ::= &lt;regex&gt; | localhost
 * regex is a regular expression with '*' as multi character and '?' as single character wild card
 * </pre>
 * <br>
 * Example: allow localhost:
 * <br>
 * new PatternRule(true, "n:localhost")
 * <br>
 * Example: allow local lan:
 * <br>
 * new PatternRule(true, "i:192.168.0.*")
 * <br>
 * Example: block all
 * <br>
 * new PatternRule(false, "n:*")
 * <br>
 * <p>
 * For some reason Netty 4 didn't copy this from Netty 3. The code was copied from the Netty 3 PatternRule
 * and modifed as required to match the new version of IpFilterRule.
 */
public class PatternRule implements IpFilterRule {

  private static final Logger logger = LoggerFactory.getLogger(PatternRule.class);
  private static final String LOCALHOST = "127.0.0.1";
  private final IpFilterRuleType ruleType;
  private Pattern ipPattern;
  private Pattern namePattern;
  private boolean localhost;

  /**
   * Construct the IpFilterRule from a pattern.
   *
   * @param ruleType The RuleType (accept or deny)
   * @param pattern  The pattern.
   */
  public PatternRule(IpFilterRuleType ruleType, String pattern) {
    this.ruleType = ruleType;
    parse(pattern);
  }

  private static String addRule(String pattern, String rule) {
    if (rule == null || rule.length() == 0) {
      return pattern;
    }
    if (pattern.length() != 0) {
      pattern += "|";
    }
    rule = rule.replaceAll("\\.", "\\\\.");
    rule = rule.replaceAll("\\*", ".*");
    rule = rule.replaceAll("\\?", ".");
    pattern += '(' + rule + ')';
    return pattern;
  }

  private static boolean isLocalhost(InetAddress address) {
    try {
      if (address.equals(InetAddress.getLocalHost())) {
        return true;
      }
    } catch (UnknownHostException e) {
      if (logger.isInfoEnabled()) {
        logger.info("error getting ip of localhost", e);
      }
    }
    try {
      InetAddress[] addrs = InetAddress.getAllByName(LOCALHOST);
      for (InetAddress addr : addrs) {
        if (addr.equals(address)) {
          return true;
        }
      }
    } catch (UnknownHostException e) {
      if (logger.isInfoEnabled()) {
        logger.info("error getting ip of localhost", e);
      }
    }
    return false;
  }

  @Override
  public boolean matches(InetSocketAddress inetSocketAddress) {
    InetAddress inetAddress = inetSocketAddress.getAddress();
    if (localhost && isLocalhost(inetAddress)) {
      return true;
    }
    if (ipPattern != null && ipPattern.matcher(inetAddress.getHostAddress()).matches()) {
      return true;
    }
    if (namePattern != null) {
      return namePattern.matcher(inetAddress.getHostName()).matches();
    }
    return false;
  }

  @Override
  public IpFilterRuleType ruleType() {
    return ruleType;
  }

  private void parse(String pattern) {
    if (pattern == null) {
      return;
    }

    String[] acls = pattern.split(",");

    String ip = "";
    String name = "";
    for (String c : acls) {
      c = c.trim();
      if ("n:localhost".equals(c)) {
        localhost = true;
      } else if (c.startsWith("n:")) {
        name = addRule(name, c.substring(2));
      } else if (c.startsWith("i:")) {
        ip = addRule(ip, c.substring(2));
      }
    }
    if (ip.length() != 0) {
      ipPattern = Pattern.compile(ip);
    }
    if (name.length() != 0) {
      namePattern = Pattern.compile(name);
    }
  }
}
