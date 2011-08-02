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

package com.cloudera.flume.conf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a library of pattern matching primitives over ANTLR's
 * CommonTree data structure. This is best used with a import static
 * ...PatternMatch.*; statement. See the test cases for clearer examples.
 * 
 * Base patterns (wild, var, kind, tuple) are generally instantiated with the
 * static method. parent relation patterns are object methods that assume that
 * 'this' is the parent.
 * 
 * Example:
 * 
 * var("matchname", wild()) // match any and bind matchname to it.
 * 
 * kind("DECO").child(var("sink",kind("SINK"))) // find (DECO ( SINK xxx) (xxx)
 * ) and bind "sink" to ( SINK xxx )
 * 
 * If there are multiple var's specified in a pattern with the same name, the
 * parent/outermost will win. If they are at the same level in a tuple, the left
 * most will win.
 * 
 * NOTE: This is tied to the AST representation (CommonTree) used in the parser
 * generator. This is not directly tied to the actual lex/parser.
 */
public class PatternMatch {
  public static final Logger LOG = LoggerFactory.getLogger(PatternMatch.class);

  enum PatternType {
    WILD, VAR, KIND, TUPLE, PARENT, PARENT_NTH, PARENT_RECURSIVE, OR
  }

  PatternType pt;
  Object[] args; // pattern arguments

  /**
   * only use the static constructors to generate patterns.
   */
  private PatternMatch(PatternType type, Object... args) {
    this.pt = type;
    this.args = args;
  }

  /**
   * Returns null if there is no match. Returns empty list if the tree matches.
   * Returns map of name->CommonTree bindings if there is a match and there were
   * pattern variables specified.
   * 
   * For bindings, we favor parent of child and left child over right child.
   */
  @SuppressWarnings("unchecked")
  public Map<String, CommonTree> match(CommonTree ct) {
    switch (pt) {
    case WILD:
      return new HashMap<String, CommonTree>();
    case VAR: { // bind a pattern variable to the pipe
      String x = (String) args[0];
      PatternMatch pv = (PatternMatch) args[1];
      Map<String, CommonTree> m = pv.match(ct);
      // Fail!
      if (m == null) {
        return null;
      }
      // This is the ONLY pattern that can bind and return
      m.put(x, ct);
      return m;
    }
    case KIND: // node kind matches
      String k = (String) args[0];
      if (ct != null && k.equals(ct.getText())) {
        return new HashMap<String, CommonTree>();
      }
      return null;
    case TUPLE: { // parent tuple
      PatternMatch parent = (PatternMatch) args[0];
      // recursive case
      if (ct == null) {
        return null;
      }

      List<CommonTree> ch = ct.getChildren();
      if (ch == null) {
        return null;
      }

      if (ch.size() + 1 != args.length) {
        return null; // tuple cardinality is wrong
      }

      Map<String, CommonTree> mps = parent.match(ct);
      if (mps == null)
        return null;

      Map<String, CommonTree> ret = new HashMap<String, CommonTree>();

      // This checks to see that each element in the tuple pattern matches the
      // corresponding element in the AST. We do this backwards to favor left
      // bindings
      for (int i = ch.size(); i >= 1; i--) {
        CommonTree e = ch.get(i - 1);
        PatternMatch p = (PatternMatch) args[i];
        Map<String, CommonTree> match = p.match(e);
        if (match != null) {
          // success!
          ret.putAll(match);
        } else {
          // element did not match, failed.
          return null;
        }
      }
      ret.putAll(mps); // parent will mask out children.
      return ret;
    }
    case PARENT: {
      PatternMatch parent = (PatternMatch) args[0];
      PatternMatch child = (PatternMatch) args[1];
      // recursive case
      if (ct == null) {
        return null;
      }
      List<CommonTree> ch = ct.getChildren();
      if (ch == null) {
        return null;
      }

      Map<String, CommonTree> ret = parent.match(ct);
      if (ret == null)
        return null;

      // this will just find the first match
      for (CommonTree e : ch) {
        Map<String, CommonTree> match = child.match(e);
        if (match != null) {
          // success!
          match.putAll(ret);
          return match;
        }
      }
      return null;
    }
    case PARENT_NTH: {
      PatternMatch parent = (PatternMatch) args[0];
      PatternMatch child = (PatternMatch) args[1];
      Integer nth = (Integer) args[2];
      // recursive case
      List<CommonTree> ch = (List<CommonTree>) ct.getChildren();
      if (ch == null) {
        return null;
      }

      Map<String, CommonTree> ret = parent.match(ct);
      if (ret == null)
        return null;

      // check the nth child to see if it matches.
      CommonTree chd = ch.get(nth);
      Map<String, CommonTree> match = child.match(chd);
      if (match != null) {
        // success!
        match.putAll(ret);
        return match;
      }

      return null;
    }

    case PARENT_RECURSIVE: {
      PatternMatch pat = (PatternMatch) args[0];
      // base case
      Map<String, CommonTree> m1 = pat.match(ct);
      if (m1 != null) {
        return m1;
      }

      if (ct == null) {
        return null;
      }

      // recursive case
      List<CommonTree> ch = (List<CommonTree>) ct.getChildren();
      if (ch == null) {
        return null;
      }
      for (CommonTree e : ch) {
        LOG.debug(e.toStringTree());
        Map<String, CommonTree> match = this.match(e);
        if (match != null) {
          // success!
          return match;
        }

      }
      return null;

    }

    case OR: {
      // this will just find the first match
      for (PatternMatch p : (PatternMatch[]) args) {
        Map<String, CommonTree> match = p.match(ct);
        if (match != null) {
          // success!
          return match;
        }
      }
      return null;
    }

    default:
      throw new IllegalStateException("Unexpected Pattern type: " + pt);
    }
  }

  /**
   * This method creates a binding pattern, that binds if p matches.
   */
  public static PatternMatch var(String name, PatternMatch p) {
    return new PatternMatch(PatternType.VAR, name, p);
  }

  /**
   * This method creates a wildcard pattern, a pattern that always matches
   */
  public static PatternMatch wild() {
    return new PatternMatch(PatternType.WILD);
  }

  /**
   * This method creates a kind pattern, a pattern that matches if the node text
   * is the same as k.
   */
  public static PatternMatch kind(String k) {
    return new PatternMatch(PatternType.KIND, k);
  }

  /**
   * This method creates a tuple pattern, a pattern that matches if the
   * cardinality of the node is the same and that each of the corresponding
   * patterns match.
   */
  public static PatternMatch tuple(PatternMatch... tuple) {
    // The first item in tuple is the parent, the subsequent nodes are the
    // children in tuple order.

    // Cast forces tuple to be interpreted as Object...
    return new PatternMatch(PatternType.TUPLE, (Object[]) tuple);
  }

  /**
   * This method creates a child relation where 'this' is the parent, and the
   * specified pattern child is a child. This matches if *any* of the parent
   * node's children match.
   */
  public PatternMatch child(PatternMatch child) {
    return new PatternMatch(PatternType.PARENT, this, child);
  }

  /**
   * This method creates an nth child pattern match relation. For this to match,
   * 'this' matches the parent, and the nth child of the parent matches the child
   * pattern.
   */
  public PatternMatch nth(int n, PatternMatch child) {
    return new PatternMatch(PatternType.PARENT_NTH, this, child, n);
  }

  /**
   * This method creates a recursive pattern match relation. This will traverse
   * through any number of nodes to check if the child pattern matches.
   */
  public static PatternMatch recursive(PatternMatch child) {
    return new PatternMatch(PatternType.PARENT_RECURSIVE, child);
  }

  /**
   * This method creates an 'or' disjunct match pattern. This will attempt each
   * disjunct in order until one matches.x
   */
  public static PatternMatch or(PatternMatch... choice) {
    return new PatternMatch(PatternType.OR, (Object[]) choice);
  }

  /**
   * This does an inplace child replacement. This replaces the current dst nodes
   * children with the src nodes children.
   */
  public static void replaceChildren(CommonTree dst, CommonTree src) {
    // delete all
    int dCount = dst.getChildCount();
    for (int i = 0; i < dCount; i++) {
      dst.deleteChild(0);
    }
    // insert all
    dst.addChildren(src.getChildren());
  }

}
