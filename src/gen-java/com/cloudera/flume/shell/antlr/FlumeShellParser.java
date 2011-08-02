// $ANTLR 3.1.3 Mar 18, 2009 10:09:25 /home/jon/flume/src/antlr/FlumeShell.g 2010-06-23 10:20:01

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


import org.antlr.runtime.tree.*;

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
public class FlumeShellParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "CMD", "DQUOTE", "SQUOTE", "STRING", "DQuoteLiteral", "SQuoteLiteral", "Argument", "HexDigit", "EscapeSequence", "UnicodeEscape", "OctalEscape", "Letter", "JavaIDDigit", "WS", "LINE_COMMENT", "';'"
    };
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

    // delegates
    // delegators


        public FlumeShellParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public FlumeShellParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return FlumeShellParser.tokenNames; }
    public String getGrammarFileName() { return "/home/jon/flume/src/antlr/FlumeShell.g"; }

     
    	public void reportError(RecognitionException re) {
    		throw new RuntimeException ("Parser Error: "+re);
    		// throw re; // TODO (jon) provide more info on a parser fail
    	}


    public static class lines_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "lines"
    // /home/jon/flume/src/antlr/FlumeShell.g:86:1: lines : command ( ';' command )* EOF -> ( command )+ ;
    public final FlumeShellParser.lines_return lines() throws RecognitionException {
        FlumeShellParser.lines_return retval = new FlumeShellParser.lines_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal2=null;
        Token EOF4=null;
        FlumeShellParser.command_return command1 = null;

        FlumeShellParser.command_return command3 = null;


        Object char_literal2_tree=null;
        Object EOF4_tree=null;
        RewriteRuleTokenStream stream_19=new RewriteRuleTokenStream(adaptor,"token 19");
        RewriteRuleTokenStream stream_EOF=new RewriteRuleTokenStream(adaptor,"token EOF");
        RewriteRuleSubtreeStream stream_command=new RewriteRuleSubtreeStream(adaptor,"rule command");
        try {
            // /home/jon/flume/src/antlr/FlumeShell.g:86:7: ( command ( ';' command )* EOF -> ( command )+ )
            // /home/jon/flume/src/antlr/FlumeShell.g:86:9: command ( ';' command )* EOF
            {
            pushFollow(FOLLOW_command_in_lines85);
            command1=command();

            state._fsp--;

            stream_command.add(command1.getTree());
            // /home/jon/flume/src/antlr/FlumeShell.g:86:17: ( ';' command )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==19) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/FlumeShell.g:86:18: ';' command
            	    {
            	    char_literal2=(Token)match(input,19,FOLLOW_19_in_lines88);  
            	    stream_19.add(char_literal2);

            	    pushFollow(FOLLOW_command_in_lines90);
            	    command3=command();

            	    state._fsp--;

            	    stream_command.add(command3.getTree());

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            EOF4=(Token)match(input,EOF,FOLLOW_EOF_in_lines94);  
            stream_EOF.add(EOF4);



            // AST REWRITE
            // elements: command
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 86:36: -> ( command )+
            {
                if ( !(stream_command.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_command.hasNext() ) {
                    adaptor.addChild(root_0, stream_command.nextTree());

                }
                stream_command.reset();

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (Object)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (Object)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "lines"

    public static class line_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "line"
    // /home/jon/flume/src/antlr/FlumeShell.g:88:1: line : command EOF -> command ;
    public final FlumeShellParser.line_return line() throws RecognitionException {
        FlumeShellParser.line_return retval = new FlumeShellParser.line_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token EOF6=null;
        FlumeShellParser.command_return command5 = null;


        Object EOF6_tree=null;
        RewriteRuleTokenStream stream_EOF=new RewriteRuleTokenStream(adaptor,"token EOF");
        RewriteRuleSubtreeStream stream_command=new RewriteRuleSubtreeStream(adaptor,"rule command");
        try {
            // /home/jon/flume/src/antlr/FlumeShell.g:88:6: ( command EOF -> command )
            // /home/jon/flume/src/antlr/FlumeShell.g:88:8: command EOF
            {
            pushFollow(FOLLOW_command_in_line107);
            command5=command();

            state._fsp--;

            stream_command.add(command5.getTree());
            EOF6=(Token)match(input,EOF,FOLLOW_EOF_in_line109);  
            stream_EOF.add(EOF6);



            // AST REWRITE
            // elements: command
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 88:20: -> command
            {
                adaptor.addChild(root_0, stream_command.nextTree());

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (Object)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (Object)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "line"

    public static class command_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "command"
    // /home/jon/flume/src/antlr/FlumeShell.g:90:1: command : ( literal )+ -> ^( CMD ( literal )+ ) ;
    public final FlumeShellParser.command_return command() throws RecognitionException {
        FlumeShellParser.command_return retval = new FlumeShellParser.command_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        FlumeShellParser.literal_return literal7 = null;


        RewriteRuleSubtreeStream stream_literal=new RewriteRuleSubtreeStream(adaptor,"rule literal");
        try {
            // /home/jon/flume/src/antlr/FlumeShell.g:90:9: ( ( literal )+ -> ^( CMD ( literal )+ ) )
            // /home/jon/flume/src/antlr/FlumeShell.g:90:11: ( literal )+
            {
            // /home/jon/flume/src/antlr/FlumeShell.g:90:11: ( literal )+
            int cnt2=0;
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( ((LA2_0>=DQuoteLiteral && LA2_0<=Argument)) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/FlumeShell.g:90:11: literal
            	    {
            	    pushFollow(FOLLOW_literal_in_command122);
            	    literal7=literal();

            	    state._fsp--;

            	    stream_literal.add(literal7.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt2 >= 1 ) break loop2;
                        EarlyExitException eee =
                            new EarlyExitException(2, input);
                        throw eee;
                }
                cnt2++;
            } while (true);



            // AST REWRITE
            // elements: literal
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 90:20: -> ^( CMD ( literal )+ )
            {
                // /home/jon/flume/src/antlr/FlumeShell.g:90:23: ^( CMD ( literal )+ )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(CMD, "CMD"), root_1);

                if ( !(stream_literal.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_literal.hasNext() ) {
                    adaptor.addChild(root_1, stream_literal.nextTree());

                }
                stream_literal.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (Object)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (Object)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "command"

    public static class literal_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "literal"
    // /home/jon/flume/src/antlr/FlumeShell.g:93:1: literal : ( DQuoteLiteral -> ^( DQUOTE DQuoteLiteral ) | SQuoteLiteral -> ^( SQUOTE SQuoteLiteral ) | Argument -> ^( STRING Argument ) );
    public final FlumeShellParser.literal_return literal() throws RecognitionException {
        FlumeShellParser.literal_return retval = new FlumeShellParser.literal_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token DQuoteLiteral8=null;
        Token SQuoteLiteral9=null;
        Token Argument10=null;

        Object DQuoteLiteral8_tree=null;
        Object SQuoteLiteral9_tree=null;
        Object Argument10_tree=null;
        RewriteRuleTokenStream stream_Argument=new RewriteRuleTokenStream(adaptor,"token Argument");
        RewriteRuleTokenStream stream_DQuoteLiteral=new RewriteRuleTokenStream(adaptor,"token DQuoteLiteral");
        RewriteRuleTokenStream stream_SQuoteLiteral=new RewriteRuleTokenStream(adaptor,"token SQuoteLiteral");

        try {
            // /home/jon/flume/src/antlr/FlumeShell.g:94:5: ( DQuoteLiteral -> ^( DQUOTE DQuoteLiteral ) | SQuoteLiteral -> ^( SQUOTE SQuoteLiteral ) | Argument -> ^( STRING Argument ) )
            int alt3=3;
            switch ( input.LA(1) ) {
            case DQuoteLiteral:
                {
                alt3=1;
                }
                break;
            case SQuoteLiteral:
                {
                alt3=2;
                }
                break;
            case Argument:
                {
                alt3=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }

            switch (alt3) {
                case 1 :
                    // /home/jon/flume/src/antlr/FlumeShell.g:94:9: DQuoteLiteral
                    {
                    DQuoteLiteral8=(Token)match(input,DQuoteLiteral,FOLLOW_DQuoteLiteral_in_literal150);  
                    stream_DQuoteLiteral.add(DQuoteLiteral8);



                    // AST REWRITE
                    // elements: DQuoteLiteral
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 94:23: -> ^( DQUOTE DQuoteLiteral )
                    {
                        // /home/jon/flume/src/antlr/FlumeShell.g:94:26: ^( DQUOTE DQuoteLiteral )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(DQUOTE, "DQUOTE"), root_1);

                        adaptor.addChild(root_1, stream_DQuoteLiteral.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/FlumeShell.g:95:9: SQuoteLiteral
                    {
                    SQuoteLiteral9=(Token)match(input,SQuoteLiteral,FOLLOW_SQuoteLiteral_in_literal170);  
                    stream_SQuoteLiteral.add(SQuoteLiteral9);



                    // AST REWRITE
                    // elements: SQuoteLiteral
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 95:23: -> ^( SQUOTE SQuoteLiteral )
                    {
                        // /home/jon/flume/src/antlr/FlumeShell.g:95:26: ^( SQUOTE SQuoteLiteral )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(SQUOTE, "SQUOTE"), root_1);

                        adaptor.addChild(root_1, stream_SQuoteLiteral.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // /home/jon/flume/src/antlr/FlumeShell.g:96:9: Argument
                    {
                    Argument10=(Token)match(input,Argument,FOLLOW_Argument_in_literal188);  
                    stream_Argument.add(Argument10);



                    // AST REWRITE
                    // elements: Argument
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 96:21: -> ^( STRING Argument )
                    {
                        // /home/jon/flume/src/antlr/FlumeShell.g:96:24: ^( STRING Argument )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(STRING, "STRING"), root_1);

                        adaptor.addChild(root_1, stream_Argument.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (Object)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (Object)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "literal"

    // Delegated rules


 

    public static final BitSet FOLLOW_command_in_lines85 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_19_in_lines88 = new BitSet(new long[]{0x0000000000000700L});
    public static final BitSet FOLLOW_command_in_lines90 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_EOF_in_lines94 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_command_in_line107 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_line109 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_command122 = new BitSet(new long[]{0x0000000000000702L});
    public static final BitSet FOLLOW_DQuoteLiteral_in_literal150 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SQuoteLiteral_in_literal170 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Argument_in_literal188 = new BitSet(new long[]{0x0000000000000002L});

}