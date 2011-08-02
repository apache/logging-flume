// $ANTLR 3.1.3 Mar 18, 2009 10:09:25 /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g 2011-02-08 16:07:08

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


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class FlumeDeployLexer extends Lexer {
    public static final int DEC=15;
    public static final int FloatTypeSuffix=35;
    public static final int OctalLiteral=25;
    public static final int SOURCE=11;
    public static final int Exponent=34;
    public static final int FLOAT=18;
    public static final int MULTI=12;
    public static final int EOF=-1;
    public static final int SINK=6;
    public static final int HexDigit=27;
    public static final int Identifier=20;
    public static final int T__55=55;
    public static final int T__56=56;
    public static final int T__57=57;
    public static final int T__51=51;
    public static final int T__52=52;
    public static final int T__53=53;
    public static final int T__54=54;
    public static final int HEX=13;
    public static final int IPLiteral=21;
    public static final int GEN=9;
    public static final int COMMENT=37;
    public static final int T__50=50;
    public static final int T__42=42;
    public static final int T__43=43;
    public static final int HexLiteral=24;
    public static final int T__40=40;
    public static final int T__41=41;
    public static final int T__46=46;
    public static final int T__47=47;
    public static final int T__44=44;
    public static final int NODE=4;
    public static final int T__45=45;
    public static final int LINE_COMMENT=38;
    public static final int IntegerTypeSuffix=28;
    public static final int T__48=48;
    public static final int T__49=49;
    public static final int ROLL=8;
    public static final int BLANK=5;
    public static final int BOOL=17;
    public static final int KWARG=19;
    public static final int DecimalLiteral=26;
    public static final int BACKUP=7;
    public static final int StringLiteral=22;
    public static final int OCT=14;
    public static final int WS=36;
    public static final int T__39=39;
    public static final int UnicodeEscape=30;
    public static final int DECO=10;
    public static final int FloatingPointLiteral=23;
    public static final int JavaIDDigit=33;
    public static final int EscapeSequence=29;
    public static final int OctalEscape=31;
    public static final int Letter=32;
    public static final int STRING=16;

    	public void reportError(RecognitionException re) {
    		throw new RuntimeRecognitionException (re);
    	}


    // delegates
    // delegators

    public FlumeDeployLexer() {;} 
    public FlumeDeployLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public FlumeDeployLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "/home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g"; }

    // $ANTLR start "T__39"
    public final void mT__39() throws RecognitionException {
        try {
            int _type = T__39;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:30:7: ( ':' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:30:9: ':'
            {
            match(':'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__39"

    // $ANTLR start "T__40"
    public final void mT__40() throws RecognitionException {
        try {
            int _type = T__40;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:31:7: ( '|' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:31:9: '|'
            {
            match('|'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__40"

    // $ANTLR start "T__41"
    public final void mT__41() throws RecognitionException {
        try {
            int _type = T__41;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:32:7: ( ';' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:32:9: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__41"

    // $ANTLR start "T__42"
    public final void mT__42() throws RecognitionException {
        try {
            int _type = T__42;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:33:7: ( ',' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:33:9: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__42"

    // $ANTLR start "T__43"
    public final void mT__43() throws RecognitionException {
        try {
            int _type = T__43;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:34:7: ( '[' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:34:9: '['
            {
            match('['); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__43"

    // $ANTLR start "T__44"
    public final void mT__44() throws RecognitionException {
        try {
            int _type = T__44;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:35:7: ( ']' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:35:9: ']'
            {
            match(']'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__44"

    // $ANTLR start "T__45"
    public final void mT__45() throws RecognitionException {
        try {
            int _type = T__45;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:36:7: ( '{' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:36:9: '{'
            {
            match('{'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__45"

    // $ANTLR start "T__46"
    public final void mT__46() throws RecognitionException {
        try {
            int _type = T__46;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:37:7: ( '}' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:37:9: '}'
            {
            match('}'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__46"

    // $ANTLR start "T__47"
    public final void mT__47() throws RecognitionException {
        try {
            int _type = T__47;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:38:7: ( '<' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:38:9: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__47"

    // $ANTLR start "T__48"
    public final void mT__48() throws RecognitionException {
        try {
            int _type = T__48;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:39:7: ( '>' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:39:9: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__48"

    // $ANTLR start "T__49"
    public final void mT__49() throws RecognitionException {
        try {
            int _type = T__49;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:40:7: ( '=>' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:40:9: '=>'
            {
            match("=>"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__49"

    // $ANTLR start "T__50"
    public final void mT__50() throws RecognitionException {
        try {
            int _type = T__50;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:41:7: ( '?' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:41:9: '?'
            {
            match('?'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__50"

    // $ANTLR start "T__51"
    public final void mT__51() throws RecognitionException {
        try {
            int _type = T__51;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:42:7: ( 'roll' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:42:9: 'roll'
            {
            match("roll"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__51"

    // $ANTLR start "T__52"
    public final void mT__52() throws RecognitionException {
        try {
            int _type = T__52;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:43:7: ( 'collector' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:43:9: 'collector'
            {
            match("collector"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__52"

    // $ANTLR start "T__53"
    public final void mT__53() throws RecognitionException {
        try {
            int _type = T__53;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:44:7: ( '(' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:44:9: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__53"

    // $ANTLR start "T__54"
    public final void mT__54() throws RecognitionException {
        try {
            int _type = T__54;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:45:7: ( ')' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:45:9: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__54"

    // $ANTLR start "T__55"
    public final void mT__55() throws RecognitionException {
        try {
            int _type = T__55;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:46:7: ( '=' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:46:9: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__55"

    // $ANTLR start "T__56"
    public final void mT__56() throws RecognitionException {
        try {
            int _type = T__56;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:47:7: ( 'true' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:47:9: 'true'
            {
            match("true"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__56"

    // $ANTLR start "T__57"
    public final void mT__57() throws RecognitionException {
        try {
            int _type = T__57;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:48:7: ( 'false' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:48:9: 'false'
            {
            match("false"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__57"

    // $ANTLR start "HexLiteral"
    public final void mHexLiteral() throws RecognitionException {
        try {
            int _type = HexLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:168:12: ( '0' ( 'x' | 'X' ) ( HexDigit )+ )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:168:14: '0' ( 'x' | 'X' ) ( HexDigit )+
            {
            match('0'); 
            if ( input.LA(1)=='X'||input.LA(1)=='x' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:168:28: ( HexDigit )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')||(LA1_0>='A' && LA1_0<='F')||(LA1_0>='a' && LA1_0<='f')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:168:28: HexDigit
            	    {
            	    mHexDigit(); 

            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "HexLiteral"

    // $ANTLR start "DecimalLiteral"
    public final void mDecimalLiteral() throws RecognitionException {
        try {
            int _type = DecimalLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:170:16: ( ( '0' | '1' .. '9' ( '0' .. '9' )* ) )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:170:18: ( '0' | '1' .. '9' ( '0' .. '9' )* )
            {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:170:18: ( '0' | '1' .. '9' ( '0' .. '9' )* )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0=='0') ) {
                alt3=1;
            }
            else if ( ((LA3_0>='1' && LA3_0<='9')) ) {
                alt3=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:170:19: '0'
                    {
                    match('0'); 

                    }
                    break;
                case 2 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:170:25: '1' .. '9' ( '0' .. '9' )*
                    {
                    matchRange('1','9'); 
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:170:34: ( '0' .. '9' )*
                    loop2:
                    do {
                        int alt2=2;
                        int LA2_0 = input.LA(1);

                        if ( ((LA2_0>='0' && LA2_0<='9')) ) {
                            alt2=1;
                        }


                        switch (alt2) {
                    	case 1 :
                    	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:170:34: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    break loop2;
                        }
                    } while (true);


                    }
                    break;

            }


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DecimalLiteral"

    // $ANTLR start "OctalLiteral"
    public final void mOctalLiteral() throws RecognitionException {
        try {
            int _type = OctalLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:172:14: ( '0' ( '0' .. '7' )+ )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:172:16: '0' ( '0' .. '7' )+
            {
            match('0'); 
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:172:20: ( '0' .. '7' )+
            int cnt4=0;
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( ((LA4_0>='0' && LA4_0<='7')) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:172:21: '0' .. '7'
            	    {
            	    matchRange('0','7'); 

            	    }
            	    break;

            	default :
            	    if ( cnt4 >= 1 ) break loop4;
                        EarlyExitException eee =
                            new EarlyExitException(4, input);
                        throw eee;
                }
                cnt4++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "OctalLiteral"

    // $ANTLR start "HexDigit"
    public final void mHexDigit() throws RecognitionException {
        try {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:175:10: ( ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' ) )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:175:12: ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' )
            {
            if ( (input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='F')||(input.LA(1)>='a' && input.LA(1)<='f') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "HexDigit"

    // $ANTLR start "IntegerTypeSuffix"
    public final void mIntegerTypeSuffix() throws RecognitionException {
        try {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:179:19: ( ( 'l' | 'L' ) )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:179:21: ( 'l' | 'L' )
            {
            if ( input.LA(1)=='L'||input.LA(1)=='l' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "IntegerTypeSuffix"

    // $ANTLR start "StringLiteral"
    public final void mStringLiteral() throws RecognitionException {
        try {
            int _type = StringLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:187:5: ( '\"' (~ ( '\\\\' | '\"' ) | EscapeSequence )* '\"' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:187:8: '\"' (~ ( '\\\\' | '\"' ) | EscapeSequence )* '\"'
            {
            match('\"'); 
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:187:12: (~ ( '\\\\' | '\"' ) | EscapeSequence )*
            loop5:
            do {
                int alt5=3;
                int LA5_0 = input.LA(1);

                if ( ((LA5_0>='\u0000' && LA5_0<='!')||(LA5_0>='#' && LA5_0<='[')||(LA5_0>=']' && LA5_0<='\uFFFF')) ) {
                    alt5=1;
                }
                else if ( (LA5_0=='\\') ) {
                    alt5=2;
                }


                switch (alt5) {
            	case 1 :
            	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:187:14: ~ ( '\\\\' | '\"' )
            	    {
            	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='!')||(input.LA(1)>='#' && input.LA(1)<='[')||(input.LA(1)>=']' && input.LA(1)<='\uFFFF') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;
            	case 2 :
            	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:187:29: EscapeSequence
            	    {
            	    mEscapeSequence(); 

            	    }
            	    break;

            	default :
            	    break loop5;
                }
            } while (true);

            match('\"'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "StringLiteral"

    // $ANTLR start "IPLiteral"
    public final void mIPLiteral() throws RecognitionException {
        try {
            int _type = IPLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:190:2: ( ( DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral ) )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:190:4: ( DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral )
            {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:190:4: ( DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:190:5: DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral '.' DecimalLiteral
            {
            mDecimalLiteral(); 
            match('.'); 
            mDecimalLiteral(); 
            match('.'); 
            mDecimalLiteral(); 
            match('.'); 
            mDecimalLiteral(); 

            }


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "IPLiteral"

    // $ANTLR start "EscapeSequence"
    public final void mEscapeSequence() throws RecognitionException {
        try {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:195:5: ( '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' ) | UnicodeEscape | OctalEscape )
            int alt6=3;
            int LA6_0 = input.LA(1);

            if ( (LA6_0=='\\') ) {
                switch ( input.LA(2) ) {
                case '\"':
                case '\'':
                case '\\':
                case 'b':
                case 'f':
                case 'n':
                case 'r':
                case 't':
                    {
                    alt6=1;
                    }
                    break;
                case 'u':
                    {
                    alt6=2;
                    }
                    break;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                    {
                    alt6=3;
                    }
                    break;
                default:
                    NoViableAltException nvae =
                        new NoViableAltException("", 6, 1, input);

                    throw nvae;
                }

            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 0, input);

                throw nvae;
            }
            switch (alt6) {
                case 1 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:195:9: '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' )
                    {
                    match('\\'); 
                    if ( input.LA(1)=='\"'||input.LA(1)=='\''||input.LA(1)=='\\'||input.LA(1)=='b'||input.LA(1)=='f'||input.LA(1)=='n'||input.LA(1)=='r'||input.LA(1)=='t' ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}


                    }
                    break;
                case 2 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:196:9: UnicodeEscape
                    {
                    mUnicodeEscape(); 

                    }
                    break;
                case 3 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:197:9: OctalEscape
                    {
                    mOctalEscape(); 

                    }
                    break;

            }
        }
        finally {
        }
    }
    // $ANTLR end "EscapeSequence"

    // $ANTLR start "OctalEscape"
    public final void mOctalEscape() throws RecognitionException {
        try {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:202:5: ( '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) )
            int alt7=3;
            int LA7_0 = input.LA(1);

            if ( (LA7_0=='\\') ) {
                int LA7_1 = input.LA(2);

                if ( ((LA7_1>='0' && LA7_1<='3')) ) {
                    int LA7_2 = input.LA(3);

                    if ( ((LA7_2>='0' && LA7_2<='7')) ) {
                        int LA7_4 = input.LA(4);

                        if ( ((LA7_4>='0' && LA7_4<='7')) ) {
                            alt7=1;
                        }
                        else {
                            alt7=2;}
                    }
                    else {
                        alt7=3;}
                }
                else if ( ((LA7_1>='4' && LA7_1<='7')) ) {
                    int LA7_3 = input.LA(3);

                    if ( ((LA7_3>='0' && LA7_3<='7')) ) {
                        alt7=2;
                    }
                    else {
                        alt7=3;}
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 7, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 7, 0, input);

                throw nvae;
            }
            switch (alt7) {
                case 1 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:202:9: '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:202:14: ( '0' .. '3' )
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:202:15: '0' .. '3'
                    {
                    matchRange('0','3'); 

                    }

                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:202:25: ( '0' .. '7' )
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:202:26: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }

                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:202:36: ( '0' .. '7' )
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:202:37: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;
                case 2 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:203:9: '\\\\' ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:203:14: ( '0' .. '7' )
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:203:15: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }

                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:203:25: ( '0' .. '7' )
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:203:26: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;
                case 3 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:204:9: '\\\\' ( '0' .. '7' )
                    {
                    match('\\'); 
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:204:14: ( '0' .. '7' )
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:204:15: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;

            }
        }
        finally {
        }
    }
    // $ANTLR end "OctalEscape"

    // $ANTLR start "UnicodeEscape"
    public final void mUnicodeEscape() throws RecognitionException {
        try {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:209:5: ( '\\\\' 'u' HexDigit HexDigit HexDigit HexDigit )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:209:9: '\\\\' 'u' HexDigit HexDigit HexDigit HexDigit
            {
            match('\\'); 
            match('u'); 
            mHexDigit(); 
            mHexDigit(); 
            mHexDigit(); 
            mHexDigit(); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "UnicodeEscape"

    // $ANTLR start "Identifier"
    public final void mIdentifier() throws RecognitionException {
        try {
            int _type = Identifier;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:213:5: ( Letter ( Letter | JavaIDDigit | '.' | '-' | '_' )* )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:213:9: Letter ( Letter | JavaIDDigit | '.' | '-' | '_' )*
            {
            mLetter(); 
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:213:16: ( Letter | JavaIDDigit | '.' | '-' | '_' )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( ((LA8_0>='-' && LA8_0<='.')||(LA8_0>='0' && LA8_0<='9')||(LA8_0>='A' && LA8_0<='Z')||LA8_0=='_'||(LA8_0>='a' && LA8_0<='z')) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:
            	    {
            	    if ( (input.LA(1)>='-' && input.LA(1)<='.')||(input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "Identifier"

    // $ANTLR start "FloatingPointLiteral"
    public final void mFloatingPointLiteral() throws RecognitionException {
        try {
            int _type = FloatingPointLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:5: ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( Exponent )? ( FloatTypeSuffix )? | '.' ( '0' .. '9' )+ ( Exponent )? ( FloatTypeSuffix )? | ( '0' .. '9' )+ Exponent ( FloatTypeSuffix )? | ( '0' .. '9' )+ FloatTypeSuffix )
            int alt19=4;
            alt19 = dfa19.predict(input);
            switch (alt19) {
                case 1 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:9: ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( Exponent )? ( FloatTypeSuffix )?
                    {
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:9: ( '0' .. '9' )+
                    int cnt9=0;
                    loop9:
                    do {
                        int alt9=2;
                        int LA9_0 = input.LA(1);

                        if ( ((LA9_0>='0' && LA9_0<='9')) ) {
                            alt9=1;
                        }


                        switch (alt9) {
                    	case 1 :
                    	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:10: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt9 >= 1 ) break loop9;
                                EarlyExitException eee =
                                    new EarlyExitException(9, input);
                                throw eee;
                        }
                        cnt9++;
                    } while (true);

                    match('.'); 
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:25: ( '0' .. '9' )*
                    loop10:
                    do {
                        int alt10=2;
                        int LA10_0 = input.LA(1);

                        if ( ((LA10_0>='0' && LA10_0<='9')) ) {
                            alt10=1;
                        }


                        switch (alt10) {
                    	case 1 :
                    	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:26: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    break loop10;
                        }
                    } while (true);

                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:37: ( Exponent )?
                    int alt11=2;
                    int LA11_0 = input.LA(1);

                    if ( (LA11_0=='E'||LA11_0=='e') ) {
                        alt11=1;
                    }
                    switch (alt11) {
                        case 1 :
                            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:37: Exponent
                            {
                            mExponent(); 

                            }
                            break;

                    }

                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:47: ( FloatTypeSuffix )?
                    int alt12=2;
                    int LA12_0 = input.LA(1);

                    if ( (LA12_0=='D'||LA12_0=='F'||LA12_0=='d'||LA12_0=='f') ) {
                        alt12=1;
                    }
                    switch (alt12) {
                        case 1 :
                            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:217:47: FloatTypeSuffix
                            {
                            mFloatTypeSuffix(); 

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:218:9: '.' ( '0' .. '9' )+ ( Exponent )? ( FloatTypeSuffix )?
                    {
                    match('.'); 
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:218:13: ( '0' .. '9' )+
                    int cnt13=0;
                    loop13:
                    do {
                        int alt13=2;
                        int LA13_0 = input.LA(1);

                        if ( ((LA13_0>='0' && LA13_0<='9')) ) {
                            alt13=1;
                        }


                        switch (alt13) {
                    	case 1 :
                    	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:218:14: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt13 >= 1 ) break loop13;
                                EarlyExitException eee =
                                    new EarlyExitException(13, input);
                                throw eee;
                        }
                        cnt13++;
                    } while (true);

                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:218:25: ( Exponent )?
                    int alt14=2;
                    int LA14_0 = input.LA(1);

                    if ( (LA14_0=='E'||LA14_0=='e') ) {
                        alt14=1;
                    }
                    switch (alt14) {
                        case 1 :
                            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:218:25: Exponent
                            {
                            mExponent(); 

                            }
                            break;

                    }

                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:218:35: ( FloatTypeSuffix )?
                    int alt15=2;
                    int LA15_0 = input.LA(1);

                    if ( (LA15_0=='D'||LA15_0=='F'||LA15_0=='d'||LA15_0=='f') ) {
                        alt15=1;
                    }
                    switch (alt15) {
                        case 1 :
                            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:218:35: FloatTypeSuffix
                            {
                            mFloatTypeSuffix(); 

                            }
                            break;

                    }


                    }
                    break;
                case 3 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:219:9: ( '0' .. '9' )+ Exponent ( FloatTypeSuffix )?
                    {
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:219:9: ( '0' .. '9' )+
                    int cnt16=0;
                    loop16:
                    do {
                        int alt16=2;
                        int LA16_0 = input.LA(1);

                        if ( ((LA16_0>='0' && LA16_0<='9')) ) {
                            alt16=1;
                        }


                        switch (alt16) {
                    	case 1 :
                    	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:219:10: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt16 >= 1 ) break loop16;
                                EarlyExitException eee =
                                    new EarlyExitException(16, input);
                                throw eee;
                        }
                        cnt16++;
                    } while (true);

                    mExponent(); 
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:219:30: ( FloatTypeSuffix )?
                    int alt17=2;
                    int LA17_0 = input.LA(1);

                    if ( (LA17_0=='D'||LA17_0=='F'||LA17_0=='d'||LA17_0=='f') ) {
                        alt17=1;
                    }
                    switch (alt17) {
                        case 1 :
                            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:219:30: FloatTypeSuffix
                            {
                            mFloatTypeSuffix(); 

                            }
                            break;

                    }


                    }
                    break;
                case 4 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:220:9: ( '0' .. '9' )+ FloatTypeSuffix
                    {
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:220:9: ( '0' .. '9' )+
                    int cnt18=0;
                    loop18:
                    do {
                        int alt18=2;
                        int LA18_0 = input.LA(1);

                        if ( ((LA18_0>='0' && LA18_0<='9')) ) {
                            alt18=1;
                        }


                        switch (alt18) {
                    	case 1 :
                    	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:220:10: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt18 >= 1 ) break loop18;
                                EarlyExitException eee =
                                    new EarlyExitException(18, input);
                                throw eee;
                        }
                        cnt18++;
                    } while (true);

                    mFloatTypeSuffix(); 

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "FloatingPointLiteral"

    // $ANTLR start "Exponent"
    public final void mExponent() throws RecognitionException {
        try {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:224:10: ( ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+ )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:224:12: ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:224:22: ( '+' | '-' )?
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( (LA20_0=='+'||LA20_0=='-') ) {
                alt20=1;
            }
            switch (alt20) {
                case 1 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}


                    }
                    break;

            }

            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:224:33: ( '0' .. '9' )+
            int cnt21=0;
            loop21:
            do {
                int alt21=2;
                int LA21_0 = input.LA(1);

                if ( ((LA21_0>='0' && LA21_0<='9')) ) {
                    alt21=1;
                }


                switch (alt21) {
            	case 1 :
            	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:224:34: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt21 >= 1 ) break loop21;
                        EarlyExitException eee =
                            new EarlyExitException(21, input);
                        throw eee;
                }
                cnt21++;
            } while (true);


            }

        }
        finally {
        }
    }
    // $ANTLR end "Exponent"

    // $ANTLR start "FloatTypeSuffix"
    public final void mFloatTypeSuffix() throws RecognitionException {
        try {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:227:17: ( ( 'f' | 'F' | 'd' | 'D' ) )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:227:19: ( 'f' | 'F' | 'd' | 'D' )
            {
            if ( input.LA(1)=='D'||input.LA(1)=='F'||input.LA(1)=='d'||input.LA(1)=='f' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "FloatTypeSuffix"

    // $ANTLR start "JavaIDDigit"
    public final void mJavaIDDigit() throws RecognitionException {
        try {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:231:2: ( '0' | '1' .. '9' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:
            {
            if ( (input.LA(1)>='0' && input.LA(1)<='9') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "JavaIDDigit"

    // $ANTLR start "Letter"
    public final void mLetter() throws RecognitionException {
        try {
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:234:9: ( 'a' .. 'z' | 'A' .. 'Z' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:
            {
            if ( (input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "Letter"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:236:5: ( ( ' ' | '\\r' | '\\t' | '\\u000C' | '\\n' ) )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:236:8: ( ' ' | '\\r' | '\\t' | '\\u000C' | '\\n' )
            {
            if ( (input.LA(1)>='\t' && input.LA(1)<='\n')||(input.LA(1)>='\f' && input.LA(1)<='\r')||input.LA(1)==' ' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            _channel=HIDDEN;

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "COMMENT"
    public final void mCOMMENT() throws RecognitionException {
        try {
            int _type = COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:240:5: ( '/*' ( options {greedy=false; } : . )* '*/' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:240:9: '/*' ( options {greedy=false; } : . )* '*/'
            {
            match("/*"); 

            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:240:14: ( options {greedy=false; } : . )*
            loop22:
            do {
                int alt22=2;
                int LA22_0 = input.LA(1);

                if ( (LA22_0=='*') ) {
                    int LA22_1 = input.LA(2);

                    if ( (LA22_1=='/') ) {
                        alt22=2;
                    }
                    else if ( ((LA22_1>='\u0000' && LA22_1<='.')||(LA22_1>='0' && LA22_1<='\uFFFF')) ) {
                        alt22=1;
                    }


                }
                else if ( ((LA22_0>='\u0000' && LA22_0<=')')||(LA22_0>='+' && LA22_0<='\uFFFF')) ) {
                    alt22=1;
                }


                switch (alt22) {
            	case 1 :
            	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:240:42: .
            	    {
            	    matchAny(); 

            	    }
            	    break;

            	default :
            	    break loop22;
                }
            } while (true);

            match("*/"); 

            _channel=HIDDEN;

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "COMMENT"

    // $ANTLR start "LINE_COMMENT"
    public final void mLINE_COMMENT() throws RecognitionException {
        try {
            int _type = LINE_COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:244:5: ( '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n' )
            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:244:7: '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n'
            {
            match("//"); 

            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:244:12: (~ ( '\\n' | '\\r' ) )*
            loop23:
            do {
                int alt23=2;
                int LA23_0 = input.LA(1);

                if ( ((LA23_0>='\u0000' && LA23_0<='\t')||(LA23_0>='\u000B' && LA23_0<='\f')||(LA23_0>='\u000E' && LA23_0<='\uFFFF')) ) {
                    alt23=1;
                }


                switch (alt23) {
            	case 1 :
            	    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:244:12: ~ ( '\\n' | '\\r' )
            	    {
            	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='\t')||(input.LA(1)>='\u000B' && input.LA(1)<='\f')||(input.LA(1)>='\u000E' && input.LA(1)<='\uFFFF') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop23;
                }
            } while (true);

            // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:244:26: ( '\\r' )?
            int alt24=2;
            int LA24_0 = input.LA(1);

            if ( (LA24_0=='\r') ) {
                alt24=1;
            }
            switch (alt24) {
                case 1 :
                    // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:244:26: '\\r'
                    {
                    match('\r'); 

                    }
                    break;

            }

            match('\n'); 
            _channel=HIDDEN;

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LINE_COMMENT"

    public void mTokens() throws RecognitionException {
        // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:8: ( T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | T__45 | T__46 | T__47 | T__48 | T__49 | T__50 | T__51 | T__52 | T__53 | T__54 | T__55 | T__56 | T__57 | HexLiteral | DecimalLiteral | OctalLiteral | StringLiteral | IPLiteral | Identifier | FloatingPointLiteral | WS | COMMENT | LINE_COMMENT )
        int alt25=29;
        alt25 = dfa25.predict(input);
        switch (alt25) {
            case 1 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:10: T__39
                {
                mT__39(); 

                }
                break;
            case 2 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:16: T__40
                {
                mT__40(); 

                }
                break;
            case 3 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:22: T__41
                {
                mT__41(); 

                }
                break;
            case 4 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:28: T__42
                {
                mT__42(); 

                }
                break;
            case 5 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:34: T__43
                {
                mT__43(); 

                }
                break;
            case 6 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:40: T__44
                {
                mT__44(); 

                }
                break;
            case 7 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:46: T__45
                {
                mT__45(); 

                }
                break;
            case 8 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:52: T__46
                {
                mT__46(); 

                }
                break;
            case 9 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:58: T__47
                {
                mT__47(); 

                }
                break;
            case 10 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:64: T__48
                {
                mT__48(); 

                }
                break;
            case 11 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:70: T__49
                {
                mT__49(); 

                }
                break;
            case 12 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:76: T__50
                {
                mT__50(); 

                }
                break;
            case 13 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:82: T__51
                {
                mT__51(); 

                }
                break;
            case 14 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:88: T__52
                {
                mT__52(); 

                }
                break;
            case 15 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:94: T__53
                {
                mT__53(); 

                }
                break;
            case 16 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:100: T__54
                {
                mT__54(); 

                }
                break;
            case 17 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:106: T__55
                {
                mT__55(); 

                }
                break;
            case 18 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:112: T__56
                {
                mT__56(); 

                }
                break;
            case 19 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:118: T__57
                {
                mT__57(); 

                }
                break;
            case 20 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:124: HexLiteral
                {
                mHexLiteral(); 

                }
                break;
            case 21 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:135: DecimalLiteral
                {
                mDecimalLiteral(); 

                }
                break;
            case 22 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:150: OctalLiteral
                {
                mOctalLiteral(); 

                }
                break;
            case 23 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:163: StringLiteral
                {
                mStringLiteral(); 

                }
                break;
            case 24 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:177: IPLiteral
                {
                mIPLiteral(); 

                }
                break;
            case 25 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:187: Identifier
                {
                mIdentifier(); 

                }
                break;
            case 26 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:198: FloatingPointLiteral
                {
                mFloatingPointLiteral(); 

                }
                break;
            case 27 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:219: WS
                {
                mWS(); 

                }
                break;
            case 28 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:222: COMMENT
                {
                mCOMMENT(); 

                }
                break;
            case 29 :
                // /home/jon/proj/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:1:230: LINE_COMMENT
                {
                mLINE_COMMENT(); 

                }
                break;

        }

    }


    protected DFA19 dfa19 = new DFA19(this);
    protected DFA25 dfa25 = new DFA25(this);
    static final String DFA19_eotS =
        "\6\uffff";
    static final String DFA19_eofS =
        "\6\uffff";
    static final String DFA19_minS =
        "\2\56\4\uffff";
    static final String DFA19_maxS =
        "\1\71\1\146\4\uffff";
    static final String DFA19_acceptS =
        "\2\uffff\1\2\1\1\1\3\1\4";
    static final String DFA19_specialS =
        "\6\uffff}>";
    static final String[] DFA19_transitionS = {
            "\1\2\1\uffff\12\1",
            "\1\3\1\uffff\12\1\12\uffff\1\5\1\4\1\5\35\uffff\1\5\1\4\1\5",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA19_eot = DFA.unpackEncodedString(DFA19_eotS);
    static final short[] DFA19_eof = DFA.unpackEncodedString(DFA19_eofS);
    static final char[] DFA19_min = DFA.unpackEncodedStringToUnsignedChars(DFA19_minS);
    static final char[] DFA19_max = DFA.unpackEncodedStringToUnsignedChars(DFA19_maxS);
    static final short[] DFA19_accept = DFA.unpackEncodedString(DFA19_acceptS);
    static final short[] DFA19_special = DFA.unpackEncodedString(DFA19_specialS);
    static final short[][] DFA19_transition;

    static {
        int numStates = DFA19_transitionS.length;
        DFA19_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA19_transition[i] = DFA.unpackEncodedString(DFA19_transitionS[i]);
        }
    }

    class DFA19 extends DFA {

        public DFA19(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 19;
            this.eot = DFA19_eot;
            this.eof = DFA19_eof;
            this.min = DFA19_min;
            this.max = DFA19_max;
            this.accept = DFA19_accept;
            this.special = DFA19_special;
            this.transition = DFA19_transition;
        }
        public String getDescription() {
            return "216:1: FloatingPointLiteral : ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( Exponent )? ( FloatTypeSuffix )? | '.' ( '0' .. '9' )+ ( Exponent )? ( FloatTypeSuffix )? | ( '0' .. '9' )+ Exponent ( FloatTypeSuffix )? | ( '0' .. '9' )+ FloatTypeSuffix );";
        }
    }
    static final String DFA25_eotS =
        "\13\uffff\1\33\1\uffff\2\26\2\uffff\2\26\2\42\7\uffff\4\26\1\uffff"+
        "\1\53\1\uffff\1\27\1\42\2\uffff\4\26\1\uffff\2\27\1\64\1\26\1\66"+
        "\1\26\1\uffff\1\27\1\uffff\1\26\1\uffff\1\71\1\26\1\uffff\2\26\1"+
        "\75\1\uffff";
    static final String DFA25_eofS =
        "\76\uffff";
    static final String DFA25_minS =
        "\1\11\12\uffff\1\76\1\uffff\2\157\2\uffff\1\162\1\141\2\56\4\uffff"+
        "\1\52\2\uffff\2\154\1\165\1\154\1\uffff\1\56\1\uffff\1\60\1\56\2"+
        "\uffff\2\154\1\145\1\163\1\uffff\2\56\1\55\1\145\1\55\1\145\1\uffff"+
        "\1\56\1\uffff\1\143\1\uffff\1\55\1\164\1\uffff\1\157\1\162\1\55"+
        "\1\uffff";
    static final String DFA25_maxS =
        "\1\175\12\uffff\1\76\1\uffff\2\157\2\uffff\1\162\1\141\1\170\1\146"+
        "\4\uffff\1\57\2\uffff\2\154\1\165\1\154\1\uffff\1\146\1\uffff\1"+
        "\71\1\146\2\uffff\2\154\1\145\1\163\1\uffff\1\56\1\71\1\172\1\145"+
        "\1\172\1\145\1\uffff\1\71\1\uffff\1\143\1\uffff\1\172\1\164\1\uffff"+
        "\1\157\1\162\1\172\1\uffff";
    static final String DFA25_acceptS =
        "\1\uffff\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\uffff\1\14"+
        "\2\uffff\1\17\1\20\4\uffff\1\27\1\31\1\32\1\33\1\uffff\1\13\1\21"+
        "\4\uffff\1\24\1\uffff\1\25\2\uffff\1\34\1\35\4\uffff\1\26\6\uffff"+
        "\1\30\1\uffff\1\15\1\uffff\1\22\2\uffff\1\23\3\uffff\1\16";
    static final String DFA25_specialS =
        "\76\uffff}>";
    static final String[] DFA25_transitionS = {
            "\2\30\1\uffff\2\30\22\uffff\1\30\1\uffff\1\25\5\uffff\1\17\1"+
            "\20\2\uffff\1\4\1\uffff\1\27\1\31\1\23\11\24\1\1\1\3\1\11\1"+
            "\13\1\12\1\14\1\uffff\32\26\1\5\1\uffff\1\6\3\uffff\2\26\1\16"+
            "\2\26\1\22\13\26\1\15\1\26\1\21\6\26\1\7\1\2\1\10",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\32",
            "",
            "\1\34",
            "\1\35",
            "",
            "",
            "\1\36",
            "\1\37",
            "\1\43\1\uffff\10\41\2\27\12\uffff\3\27\21\uffff\1\40\13\uffff"+
            "\3\27\21\uffff\1\40",
            "\1\43\1\uffff\12\44\12\uffff\3\27\35\uffff\3\27",
            "",
            "",
            "",
            "",
            "\1\45\4\uffff\1\46",
            "",
            "",
            "\1\47",
            "\1\50",
            "\1\51",
            "\1\52",
            "",
            "\1\27\1\uffff\10\41\2\27\12\uffff\3\27\35\uffff\3\27",
            "",
            "\1\54\11\55",
            "\1\43\1\uffff\12\44\12\uffff\3\27\35\uffff\3\27",
            "",
            "",
            "\1\56",
            "\1\57",
            "\1\60",
            "\1\61",
            "",
            "\1\62",
            "\1\62\1\uffff\12\63",
            "\2\26\1\uffff\12\26\7\uffff\32\26\4\uffff\1\26\1\uffff\32\26",
            "\1\65",
            "\2\26\1\uffff\12\26\7\uffff\32\26\4\uffff\1\26\1\uffff\32\26",
            "\1\67",
            "",
            "\1\62\1\uffff\12\63",
            "",
            "\1\70",
            "",
            "\2\26\1\uffff\12\26\7\uffff\32\26\4\uffff\1\26\1\uffff\32\26",
            "\1\72",
            "",
            "\1\73",
            "\1\74",
            "\2\26\1\uffff\12\26\7\uffff\32\26\4\uffff\1\26\1\uffff\32\26",
            ""
    };

    static final short[] DFA25_eot = DFA.unpackEncodedString(DFA25_eotS);
    static final short[] DFA25_eof = DFA.unpackEncodedString(DFA25_eofS);
    static final char[] DFA25_min = DFA.unpackEncodedStringToUnsignedChars(DFA25_minS);
    static final char[] DFA25_max = DFA.unpackEncodedStringToUnsignedChars(DFA25_maxS);
    static final short[] DFA25_accept = DFA.unpackEncodedString(DFA25_acceptS);
    static final short[] DFA25_special = DFA.unpackEncodedString(DFA25_specialS);
    static final short[][] DFA25_transition;

    static {
        int numStates = DFA25_transitionS.length;
        DFA25_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA25_transition[i] = DFA.unpackEncodedString(DFA25_transitionS[i]);
        }
    }

    class DFA25 extends DFA {

        public DFA25(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 25;
            this.eot = DFA25_eot;
            this.eof = DFA25_eof;
            this.min = DFA25_min;
            this.max = DFA25_max;
            this.accept = DFA25_accept;
            this.special = DFA25_special;
            this.transition = DFA25_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | T__45 | T__46 | T__47 | T__48 | T__49 | T__50 | T__51 | T__52 | T__53 | T__54 | T__55 | T__56 | T__57 | HexLiteral | DecimalLiteral | OctalLiteral | StringLiteral | IPLiteral | Identifier | FloatingPointLiteral | WS | COMMENT | LINE_COMMENT );";
        }
    }
 

}