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

// This is an ANTLR grammar for a simple deployment specification language for flume.
grammar FlumeShell;
options { 
output=AST; 
k=2;
}
tokens { 
    CMD;
    DQUOTE;
    SQUOTE;
	STRING;
}
@header {
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

package com.cloudera.flume.shell.antlr; 
}
@members{ 
	public void reportError(RecognitionException re) {
		throw new RuntimeException ("Parser Error: "+re);
		// throw re; // TODO (jon) provide more info on a parser fail
	}
}
@lexer::header {
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

package com.cloudera.flume.shell.antlr; 
}
@lexer::members {
	public void reportError(RecognitionException re) {
		throw new RuntimeException ("Lexer Error: "+re);
		// throw re; // TODO (jon) provide more info on lexer fail
	}
}

lines	:	command (';' command)* EOF -> command+;

line : command EOF -> command ;

command : literal+ -> ^(CMD literal+) ; 

// Basic Java-style literals  (taken from Java grammar)
literal 
    :   DQuoteLiteral -> ^(DQUOTE DQuoteLiteral)		
    |   SQuoteLiteral -> ^(SQUOTE SQuoteLiteral)
    |   Argument    -> ^(STRING Argument)
    ;

// LEXER

// TODO (jon) not sure why one order works but the other does not!
//DQuoteLiteral
//   :  '"' ( EscapeSequence | ~('\\'|'"') )* '"' ;


fragment
HexDigit : ('0'..'9'|'a'..'f'|'A'..'F') ;


// Double quote literals, allows for escape sequences.
DQuoteLiteral
    :  '"' ( ~('\\'|'"')  | EscapeSequence )* '"' ;

// Single quote literals, allows for " and does not escape sequences
SQuoteLiteral
    :  '\'' ( ~'\'' )* '\'' ;

  
fragment
EscapeSequence
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UnicodeEscape
    |   OctalEscape
    ;

fragment
OctalEscape
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UnicodeEscape
    :   '\\' 'u' HexDigit HexDigit HexDigit HexDigit
    ;
    
Argument
    : (Letter|JavaIDDigit|':'|'.'|'-'|'_')+
    ;

fragment
JavaIDDigit 
       :        '0' | '1'..'9' ;

fragment
Letter 	:	 'a'..'z' | 'A'..'Z';

WS  :  (' '|'\r'|'\t'|'\u000C'|'\n') {$channel=HIDDEN;}
    ;
 

LINE_COMMENT
    : '#' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    ;

