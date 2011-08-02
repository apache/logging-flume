// $ANTLR 3.1.3 Mar 18, 2009 10:09:25 /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g 2010-08-18 11:16:21

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


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class FlumeShellLexer extends Lexer {
    public static final int DQuoteLiteral=8;
    public static final int LINE_COMMENT=18;
    public static final int CMD=4;
    public static final int DQUOTE=5;
    public static final int SQUOTE=6;
    public static final int EOF=-1;
    public static final int HexDigit=11;
    public static final int T__19=19;
    public static final int WS=17;
    public static final int UnicodeEscape=13;
    public static final int SQuoteLiteral=9;
    public static final int JavaIDDigit=16;
    public static final int Argument=10;
    public static final int EscapeSequence=12;
    public static final int OctalEscape=14;
    public static final int Letter=15;
    public static final int STRING=7;

    	public void reportError(RecognitionException re) {
    		throw new RuntimeException ("Lexer Error: "+re);
    		// throw re; // TODO (jon) provide more info on lexer fail
    	}


    // delegates
    // delegators

    public FlumeShellLexer() {;} 
    public FlumeShellLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public FlumeShellLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "/home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g"; }

    // $ANTLR start "T__19"
    public final void mT__19() throws RecognitionException {
        try {
            int _type = T__19;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:31:7: ( ';' )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:31:9: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__19"

    // $ANTLR start "HexDigit"
    public final void mHexDigit() throws RecognitionException {
        try {
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:107:10: ( ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' ) )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:107:12: ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' )
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

    // $ANTLR start "DQuoteLiteral"
    public final void mDQuoteLiteral() throws RecognitionException {
        try {
            int _type = DQuoteLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:112:5: ( '\"' (~ ( '\\\\' | '\"' ) | EscapeSequence )* '\"' )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:112:8: '\"' (~ ( '\\\\' | '\"' ) | EscapeSequence )* '\"'
            {
            match('\"'); 
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:112:12: (~ ( '\\\\' | '\"' ) | EscapeSequence )*
            loop1:
            do {
                int alt1=3;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='\u0000' && LA1_0<='!')||(LA1_0>='#' && LA1_0<='[')||(LA1_0>=']' && LA1_0<='\uFFFF')) ) {
                    alt1=1;
                }
                else if ( (LA1_0=='\\') ) {
                    alt1=2;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:112:14: ~ ( '\\\\' | '\"' )
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
            	    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:112:29: EscapeSequence
            	    {
            	    mEscapeSequence(); 

            	    }
            	    break;

            	default :
            	    break loop1;
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
    // $ANTLR end "DQuoteLiteral"

    // $ANTLR start "SQuoteLiteral"
    public final void mSQuoteLiteral() throws RecognitionException {
        try {
            int _type = SQuoteLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:116:5: ( '\\'' (~ '\\'' )* '\\'' )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:116:8: '\\'' (~ '\\'' )* '\\''
            {
            match('\''); 
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:116:13: (~ '\\'' )*
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( ((LA2_0>='\u0000' && LA2_0<='&')||(LA2_0>='(' && LA2_0<='\uFFFF')) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:116:15: ~ '\\''
            	    {
            	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='&')||(input.LA(1)>='(' && input.LA(1)<='\uFFFF') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop2;
                }
            } while (true);

            match('\''); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "SQuoteLiteral"

    // $ANTLR start "EscapeSequence"
    public final void mEscapeSequence() throws RecognitionException {
        try {
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:121:5: ( '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' ) | UnicodeEscape | OctalEscape )
            int alt3=3;
            int LA3_0 = input.LA(1);

            if ( (LA3_0=='\\') ) {
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
                    alt3=1;
                    }
                    break;
                case 'u':
                    {
                    alt3=2;
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
                    alt3=3;
                    }
                    break;
                default:
                    NoViableAltException nvae =
                        new NoViableAltException("", 3, 1, input);

                    throw nvae;
                }

            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:121:9: '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' )
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
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:122:9: UnicodeEscape
                    {
                    mUnicodeEscape(); 

                    }
                    break;
                case 3 :
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:123:9: OctalEscape
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
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:128:5: ( '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) )
            int alt4=3;
            int LA4_0 = input.LA(1);

            if ( (LA4_0=='\\') ) {
                int LA4_1 = input.LA(2);

                if ( ((LA4_1>='0' && LA4_1<='3')) ) {
                    int LA4_2 = input.LA(3);

                    if ( ((LA4_2>='0' && LA4_2<='7')) ) {
                        int LA4_5 = input.LA(4);

                        if ( ((LA4_5>='0' && LA4_5<='7')) ) {
                            alt4=1;
                        }
                        else {
                            alt4=2;}
                    }
                    else {
                        alt4=3;}
                }
                else if ( ((LA4_1>='4' && LA4_1<='7')) ) {
                    int LA4_3 = input.LA(3);

                    if ( ((LA4_3>='0' && LA4_3<='7')) ) {
                        alt4=2;
                    }
                    else {
                        alt4=3;}
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 4, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;
            }
            switch (alt4) {
                case 1 :
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:128:9: '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:128:14: ( '0' .. '3' )
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:128:15: '0' .. '3'
                    {
                    matchRange('0','3'); 

                    }

                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:128:25: ( '0' .. '7' )
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:128:26: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }

                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:128:36: ( '0' .. '7' )
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:128:37: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;
                case 2 :
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:129:9: '\\\\' ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:129:14: ( '0' .. '7' )
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:129:15: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }

                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:129:25: ( '0' .. '7' )
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:129:26: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;
                case 3 :
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:130:9: '\\\\' ( '0' .. '7' )
                    {
                    match('\\'); 
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:130:14: ( '0' .. '7' )
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:130:15: '0' .. '7'
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
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:135:5: ( '\\\\' 'u' HexDigit HexDigit HexDigit HexDigit )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:135:9: '\\\\' 'u' HexDigit HexDigit HexDigit HexDigit
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

    // $ANTLR start "Argument"
    public final void mArgument() throws RecognitionException {
        try {
            int _type = Argument;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:139:5: ( ( Letter | JavaIDDigit | ':' | '.' | '-' | '_' )+ )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:139:7: ( Letter | JavaIDDigit | ':' | '.' | '-' | '_' )+
            {
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:139:7: ( Letter | JavaIDDigit | ':' | '.' | '-' | '_' )+
            int cnt5=0;
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( ((LA5_0>='-' && LA5_0<='.')||(LA5_0>='0' && LA5_0<=':')||(LA5_0>='A' && LA5_0<='Z')||LA5_0=='_'||(LA5_0>='a' && LA5_0<='z')) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:
            	    {
            	    if ( (input.LA(1)>='-' && input.LA(1)<='.')||(input.LA(1)>='0' && input.LA(1)<=':')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    if ( cnt5 >= 1 ) break loop5;
                        EarlyExitException eee =
                            new EarlyExitException(5, input);
                        throw eee;
                }
                cnt5++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "Argument"

    // $ANTLR start "JavaIDDigit"
    public final void mJavaIDDigit() throws RecognitionException {
        try {
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:144:8: ( '0' | '1' .. '9' )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:
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
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:147:9: ( 'a' .. 'z' | 'A' .. 'Z' )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:
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
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:149:5: ( ( ' ' | '\\r' | '\\t' | '\\u000C' | '\\n' ) )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:149:8: ( ' ' | '\\r' | '\\t' | '\\u000C' | '\\n' )
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

    // $ANTLR start "LINE_COMMENT"
    public final void mLINE_COMMENT() throws RecognitionException {
        try {
            int _type = LINE_COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:154:5: ( '#' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n' )
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:154:7: '#' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n'
            {
            match('#'); 
            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:154:11: (~ ( '\\n' | '\\r' ) )*
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( ((LA6_0>='\u0000' && LA6_0<='\t')||(LA6_0>='\u000B' && LA6_0<='\f')||(LA6_0>='\u000E' && LA6_0<='\uFFFF')) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:154:11: ~ ( '\\n' | '\\r' )
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
            	    break loop6;
                }
            } while (true);

            // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:154:25: ( '\\r' )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0=='\r') ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:154:25: '\\r'
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
        // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:1:8: ( T__19 | DQuoteLiteral | SQuoteLiteral | Argument | WS | LINE_COMMENT )
        int alt8=6;
        switch ( input.LA(1) ) {
        case ';':
            {
            alt8=1;
            }
            break;
        case '\"':
            {
            alt8=2;
            }
            break;
        case '\'':
            {
            alt8=3;
            }
            break;
        case '-':
        case '.':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        case ':':
        case 'A':
        case 'B':
        case 'C':
        case 'D':
        case 'E':
        case 'F':
        case 'G':
        case 'H':
        case 'I':
        case 'J':
        case 'K':
        case 'L':
        case 'M':
        case 'N':
        case 'O':
        case 'P':
        case 'Q':
        case 'R':
        case 'S':
        case 'T':
        case 'U':
        case 'V':
        case 'W':
        case 'X':
        case 'Y':
        case 'Z':
        case '_':
        case 'a':
        case 'b':
        case 'c':
        case 'd':
        case 'e':
        case 'f':
        case 'g':
        case 'h':
        case 'i':
        case 'j':
        case 'k':
        case 'l':
        case 'm':
        case 'n':
        case 'o':
        case 'p':
        case 'q':
        case 'r':
        case 's':
        case 't':
        case 'u':
        case 'v':
        case 'w':
        case 'x':
        case 'y':
        case 'z':
            {
            alt8=4;
            }
            break;
        case '\t':
        case '\n':
        case '\f':
        case '\r':
        case ' ':
            {
            alt8=5;
            }
            break;
        case '#':
            {
            alt8=6;
            }
            break;
        default:
            NoViableAltException nvae =
                new NoViableAltException("", 8, 0, input);

            throw nvae;
        }

        switch (alt8) {
            case 1 :
                // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:1:10: T__19
                {
                mT__19(); 

                }
                break;
            case 2 :
                // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:1:16: DQuoteLiteral
                {
                mDQuoteLiteral(); 

                }
                break;
            case 3 :
                // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:1:30: SQuoteLiteral
                {
                mSQuoteLiteral(); 

                }
                break;
            case 4 :
                // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:1:44: Argument
                {
                mArgument(); 

                }
                break;
            case 5 :
                // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:1:53: WS
                {
                mWS(); 

                }
                break;
            case 6 :
                // /home/patrick/Documents/flume-git2/flume/src/antlr/FlumeShell.g:1:56: LINE_COMMENT
                {
                mLINE_COMMENT(); 

                }
                break;

        }

    }


 

}