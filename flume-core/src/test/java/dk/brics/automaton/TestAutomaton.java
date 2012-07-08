/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package dk.brics.automaton;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

/**
 * This is testing an alternate regex library. Instead of java's default nfa
 * based regex automaton, this version uses a dfa. What this means is more
 * memory and time is spend up front building the dfa, but less memory and less
 * cpu is required during evaluation.
 * 
 * Importantly, because it doesn't maintain state, it loses group extraction
 * which makes this better as a filter as opposed to an extractor.
 * 
 * This uses automaton.jar from http://www.brics.dk/automaton/
 * 
 * A good summary is here :
 * http://weblogs.java.net/blog/tomwhite/archive/2006/03/a_faster_java_r.html
 * 
 * It has a BSD license.
 */
public class TestAutomaton {

  @Test
  public void testJdkRegex() {
    Pattern p = Pattern.compile("a|ab");
    Matcher m = p.matcher("ab");
    Assert.assertTrue(m.matches());
  }

  @Test
  public void testAutomatonRegex() {
    RegExp re = new RegExp("a|ab");
    RunAutomaton ra = new RunAutomaton(re.toAutomaton());

    Assert.assertTrue(ra.run("ab"));
  }

}
