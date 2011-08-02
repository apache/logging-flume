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
grammar FlumeDeploy;
options { 
output=AST; 
k=2;
}
tokens { 
  NODE;
  BLANK;
  SINK;
  BACKUP;
  ROLL;
  GEN;
  DECO;
  SOURCE;
  MULTI;
  HEX;
  OCT;
  DEC;
  STRING;
  BOOL;
  FLOAT;
  KWARG;
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

package com.cloudera.flume.conf; 
}
@members{ 
	public void reportError(RecognitionException re) {
		throw new RuntimeRecognitionException (re);
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

package com.cloudera.flume.conf; 
}
@lexer::members {
	public void reportError(RecognitionException re) {
		throw new RuntimeRecognitionException (re);
	}
}

deflist	:	def* EOF  ;

def	:	host ':' source '|' sink  ';' -> ^(NODE host source sink);

host: Identifier | IPLiteral ;

// This is currently a place holder for nodes that have multiple
// independent source sink pairs.
connection
	:	 source '|' sink -> ^(NODE BLANK source sink);

source 		:	singleSource		-> singleSource ;
sourceEof	: 	source EOF 		-> source;
singleSource	:	Identifier args?	-> ^(SOURCE Identifier args?);
multiSource	:	singleSource (',' singleSource)* -> singleSource+ ;

	
sink		:	simpleSink -> simpleSink;

singleSink	:	Identifier args? 	-> ^(SINK Identifier args?);

sinkEof		:	simpleSink EOF;

simpleSink	:	'[' multiSink ']'  	-> ^(MULTI multiSink) 
        |   singleSink simpleSink?  -> ^(DECO singleSink simpleSink?) 
		|	'{' decoratedSink '}'	-> ^(DECO decoratedSink)
		|	'<' failoverSink '>'	-> ^(BACKUP failoverSink)
        |   rollSink                -> rollSink
        |   genCollectorSink        -> genCollectorSink
		; 
 			

decoratedSink   :  singleSink '=>' sink	 		-> singleSink sink;
multiSink       :  simpleSink (',' simpleSink)* 	-> simpleSink* ;
failoverSink    :  simpleSink ('?' simpleSink)+	-> simpleSink+;	
rollSink        :  'roll' args '{' simpleSink '}'
                                  -> ^(ROLL simpleSink args);
genCollectorSink       :  'collector' args '{' simpleSink '}'
                                  -> ^(GEN 'collector' simpleSink args?);


args    : '(' ( arglist (',' kwarglist)?  ) ')' -> arglist kwarglist?
        | '(' kwarglist ')' -> kwarglist? 
        | '(' ')' -> 
        ;

arglist	:	literal (',' literal)* -> literal+ ;

kwarglist : kwarg (',' kwarg)* -> kwarg+;

kwarg   :   Identifier '=' literal -> ^(KWARG Identifier literal)  ;

// Basic Java-style literals  (taken from Java grammar)
literal
    :   integerLiteral
    |   StringLiteral		-> ^(STRING StringLiteral)
    |   booleanLiteral
    |   FloatingPointLiteral	-> ^(FLOAT FloatingPointLiteral)
    ;
integerLiteral
    :   HexLiteral 	-> ^(HEX HexLiteral)
    |   OctalLiteral 	-> ^(OCT OctalLiteral)
    |   DecimalLiteral 	-> ^(DEC DecimalLiteral)    
    ;

booleanLiteral
    :   'true' 		-> ^(BOOL 'true')
    |   'false' 	-> ^(BOOL 'false')
    ;

// LEXER

HexLiteral : '0' ('x'|'X') HexDigit+ ; //IntegerTypeSuffix? ;

DecimalLiteral : ('0' | '1'..'9' '0'..'9'*) ; //IntegerTypeSuffix? ;

OctalLiteral : '0' ('0'..'7')+ ; //IntegerTypeSuffix? ;

fragment
HexDigit : ('0'..'9'|'a'..'f'|'A'..'F') ;

// All integer values are assumed to be longs, thus don't need Long suffixes.
fragment
IntegerTypeSuffix : ('l'|'L') ;

// TODO (jon) not sure why one order works but the other does not!
//StringLiteral
//   :  '"' ( EscapeSequence | ~('\\'|'"') )* '"' ;


StringLiteral
    :  '"' ( ~('\\'|'"')  | EscapeSequence )* '"' ;

IPLiteral 
	: (DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral) ; 
 
 
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
    
Identifier 
    :   Letter (Letter|JavaIDDigit|'.'|'-'|'_')*
    ;

FloatingPointLiteral
    :   ('0'..'9')+ '.' ('0'..'9')* Exponent? FloatTypeSuffix?
    |   '.' ('0'..'9')+ Exponent? FloatTypeSuffix?
    |   ('0'..'9')+ Exponent FloatTypeSuffix?
    |   ('0'..'9')+ FloatTypeSuffix
    ;

fragment
Exponent : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

fragment
FloatTypeSuffix : ('f'|'F'|'d'|'D') ;

fragment
JavaIDDigit 
	:	 '0' | '1'..'9' ;
    
fragment
Letter 	:	 'a'..'z' | 'A'..'Z';

WS  :  (' '|'\r'|'\t'|'\u000C'|'\n') {$channel=HIDDEN;}
    ;

COMMENT
    :   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;

LINE_COMMENT
    : '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    ;

