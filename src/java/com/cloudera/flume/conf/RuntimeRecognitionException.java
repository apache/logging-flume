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

import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.lang.StringEscapeUtils;

import com.google.common.base.Preconditions;

/**
 * This exception is thrown by the antlr parser in its parse error function.
 * antlr requires us to throw a runtime exception, so we wrap the normal
 * exception inside a runtime one. This gives us fail fast behavior (which is
 * reasonable for the small configurations we have).
 */
public class RuntimeRecognitionException extends RuntimeException {

  private static final long serialVersionUID = 1L;
  RecognitionException re;

  public RuntimeRecognitionException(RecognitionException re) {
    Preconditions.checkArgument(re != null);
    this.re = re;
  }

  public RecognitionException getExn() {
    return re;
  }

  /**
   * Translate antlr internal exceptions to sane flume data flow configuration
   * specific messages.
   */
  @Override
  public String getMessage() {
    if (re instanceof NoViableAltException) {
      NoViableAltException nvae = (NoViableAltException) re;
      String c = StringEscapeUtils.escapeJava("" + (char) nvae.c);
      return "Lexer error at char '" + c + "' at line " + nvae.line + " char "
          + nvae.charPositionInLine;
    }

    if (re instanceof MismatchedTokenException) {
      MismatchedTokenException mte = (MismatchedTokenException) re;
      return "Parser error: unexpected '" + mte.token.getText()
          + "' at position " + mte.charPositionInLine + " in line '"
          + mte.input + "'";
    }

    return "Unknown RecognitionException: " + re.getMessage();
  }
}
