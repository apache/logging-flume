// $ANTLR 3.1.3 Mar 18, 2009 10:09:25 /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g 2010-06-17 15:09:39

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
public class FlumeDeployParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "NODE", "BLANK", "SINK", "BACKUP", "LET", "ROLL", "FAILCHAIN", "DECO", "SOURCE", "MULTI", "HEX", "OCT", "DEC", "STRING", "BOOL", "FLOAT", "Identifier", "IPLiteral", "StringLiteral", "FloatingPointLiteral", "HexLiteral", "OctalLiteral", "DecimalLiteral", "HexDigit", "IntegerTypeSuffix", "EscapeSequence", "UnicodeEscape", "OctalEscape", "Letter", "JavaIDDigit", "Exponent", "FloatTypeSuffix", "WS", "COMMENT", "LINE_COMMENT", "':'", "'|'", "';'", "'['", "']'", "','", "'{'", "'}'", "'<'", "'>'", "'=>'", "'?'", "'let'", "':='", "'in'", "'roll'", "'failchain'", "'('", "')'", "'true'", "'false'"
    };
    public static final int DEC=16;
    public static final int FloatTypeSuffix=35;
    public static final int OctalLiteral=25;
    public static final int Exponent=34;
    public static final int SOURCE=12;
    public static final int FLOAT=19;
    public static final int MULTI=13;
    public static final int EOF=-1;
    public static final int HexDigit=27;
    public static final int SINK=6;
    public static final int Identifier=20;
    public static final int T__55=55;
    public static final int T__56=56;
    public static final int T__57=57;
    public static final int T__58=58;
    public static final int T__51=51;
    public static final int T__52=52;
    public static final int T__53=53;
    public static final int T__54=54;
    public static final int HEX=14;
    public static final int IPLiteral=21;
    public static final int T__59=59;
    public static final int COMMENT=37;
    public static final int T__50=50;
    public static final int T__42=42;
    public static final int HexLiteral=24;
    public static final int T__43=43;
    public static final int T__40=40;
    public static final int FAILCHAIN=10;
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
    public static final int ROLL=9;
    public static final int BLANK=5;
    public static final int BOOL=18;
    public static final int DecimalLiteral=26;
    public static final int BACKUP=7;
    public static final int OCT=15;
    public static final int StringLiteral=22;
    public static final int WS=36;
    public static final int T__39=39;
    public static final int UnicodeEscape=30;
    public static final int DECO=11;
    public static final int FloatingPointLiteral=23;
    public static final int JavaIDDigit=33;
    public static final int Letter=32;
    public static final int OctalEscape=31;
    public static final int EscapeSequence=29;
    public static final int LET=8;
    public static final int STRING=17;

    // delegates
    // delegators


        public FlumeDeployParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public FlumeDeployParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return FlumeDeployParser.tokenNames; }
    public String getGrammarFileName() { return "/home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g"; }

     
    	public void reportError(RecognitionException re) {
    		throw new RuntimeRecognitionException (re);
    	}


    public static class deflist_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "deflist"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:96:1: deflist : ( def )* EOF ;
    public final FlumeDeployParser.deflist_return deflist() throws RecognitionException {
        FlumeDeployParser.deflist_return retval = new FlumeDeployParser.deflist_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token EOF2=null;
        FlumeDeployParser.def_return def1 = null;


        Object EOF2_tree=null;

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:96:9: ( ( def )* EOF )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:96:11: ( def )* EOF
            {
            root_0 = (Object)adaptor.nil();

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:96:11: ( def )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>=Identifier && LA1_0<=IPLiteral)) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:96:11: def
            	    {
            	    pushFollow(FOLLOW_def_in_deflist140);
            	    def1=def();

            	    state._fsp--;

            	    adaptor.addChild(root_0, def1.getTree());

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            EOF2=(Token)match(input,EOF,FOLLOW_EOF_in_deflist143); 
            EOF2_tree = (Object)adaptor.create(EOF2);
            adaptor.addChild(root_0, EOF2_tree);


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
    // $ANTLR end "deflist"

    public static class def_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "def"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:98:1: def : host ':' source '|' sink ';' -> ^( NODE host source sink ) ;
    public final FlumeDeployParser.def_return def() throws RecognitionException {
        FlumeDeployParser.def_return retval = new FlumeDeployParser.def_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal4=null;
        Token char_literal6=null;
        Token char_literal8=null;
        FlumeDeployParser.host_return host3 = null;

        FlumeDeployParser.source_return source5 = null;

        FlumeDeployParser.sink_return sink7 = null;


        Object char_literal4_tree=null;
        Object char_literal6_tree=null;
        Object char_literal8_tree=null;
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleSubtreeStream stream_host=new RewriteRuleSubtreeStream(adaptor,"rule host");
        RewriteRuleSubtreeStream stream_source=new RewriteRuleSubtreeStream(adaptor,"rule source");
        RewriteRuleSubtreeStream stream_sink=new RewriteRuleSubtreeStream(adaptor,"rule sink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:98:5: ( host ':' source '|' sink ';' -> ^( NODE host source sink ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:98:7: host ':' source '|' sink ';'
            {
            pushFollow(FOLLOW_host_in_def153);
            host3=host();

            state._fsp--;

            stream_host.add(host3.getTree());
            char_literal4=(Token)match(input,39,FOLLOW_39_in_def155);  
            stream_39.add(char_literal4);

            pushFollow(FOLLOW_source_in_def157);
            source5=source();

            state._fsp--;

            stream_source.add(source5.getTree());
            char_literal6=(Token)match(input,40,FOLLOW_40_in_def159);  
            stream_40.add(char_literal6);

            pushFollow(FOLLOW_sink_in_def161);
            sink7=sink();

            state._fsp--;

            stream_sink.add(sink7.getTree());
            char_literal8=(Token)match(input,41,FOLLOW_41_in_def164);  
            stream_41.add(char_literal8);



            // AST REWRITE
            // elements: source, host, sink
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 98:37: -> ^( NODE host source sink )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:98:40: ^( NODE host source sink )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(NODE, "NODE"), root_1);

                adaptor.addChild(root_1, stream_host.nextTree());
                adaptor.addChild(root_1, stream_source.nextTree());
                adaptor.addChild(root_1, stream_sink.nextTree());

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
    // $ANTLR end "def"

    public static class host_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "host"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:100:1: host : ( Identifier | IPLiteral );
    public final FlumeDeployParser.host_return host() throws RecognitionException {
        FlumeDeployParser.host_return retval = new FlumeDeployParser.host_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token set9=null;

        Object set9_tree=null;

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:100:5: ( Identifier | IPLiteral )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:
            {
            root_0 = (Object)adaptor.nil();

            set9=(Token)input.LT(1);
            if ( (input.LA(1)>=Identifier && input.LA(1)<=IPLiteral) ) {
                input.consume();
                adaptor.addChild(root_0, (Object)adaptor.create(set9));
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


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
    // $ANTLR end "host"

    public static class connection_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "connection"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:104:1: connection : source '|' sink -> ^( NODE BLANK source sink ) ;
    public final FlumeDeployParser.connection_return connection() throws RecognitionException {
        FlumeDeployParser.connection_return retval = new FlumeDeployParser.connection_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal11=null;
        FlumeDeployParser.source_return source10 = null;

        FlumeDeployParser.sink_return sink12 = null;


        Object char_literal11_tree=null;
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleSubtreeStream stream_source=new RewriteRuleSubtreeStream(adaptor,"rule source");
        RewriteRuleSubtreeStream stream_sink=new RewriteRuleSubtreeStream(adaptor,"rule sink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:105:2: ( source '|' sink -> ^( NODE BLANK source sink ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:105:5: source '|' sink
            {
            pushFollow(FOLLOW_source_in_connection200);
            source10=source();

            state._fsp--;

            stream_source.add(source10.getTree());
            char_literal11=(Token)match(input,40,FOLLOW_40_in_connection202);  
            stream_40.add(char_literal11);

            pushFollow(FOLLOW_sink_in_connection204);
            sink12=sink();

            state._fsp--;

            stream_sink.add(sink12.getTree());


            // AST REWRITE
            // elements: sink, source
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 105:21: -> ^( NODE BLANK source sink )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:105:24: ^( NODE BLANK source sink )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(NODE, "NODE"), root_1);

                adaptor.addChild(root_1, (Object)adaptor.create(BLANK, "BLANK"));
                adaptor.addChild(root_1, stream_source.nextTree());
                adaptor.addChild(root_1, stream_sink.nextTree());

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
    // $ANTLR end "connection"

    public static class source_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "source"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:107:1: source : ( singleSource -> singleSource | '[' multiSource ']' -> ^( MULTI multiSource ) );
    public final FlumeDeployParser.source_return source() throws RecognitionException {
        FlumeDeployParser.source_return retval = new FlumeDeployParser.source_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal14=null;
        Token char_literal16=null;
        FlumeDeployParser.singleSource_return singleSource13 = null;

        FlumeDeployParser.multiSource_return multiSource15 = null;


        Object char_literal14_tree=null;
        Object char_literal16_tree=null;
        RewriteRuleTokenStream stream_43=new RewriteRuleTokenStream(adaptor,"token 43");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleSubtreeStream stream_multiSource=new RewriteRuleSubtreeStream(adaptor,"rule multiSource");
        RewriteRuleSubtreeStream stream_singleSource=new RewriteRuleSubtreeStream(adaptor,"rule singleSource");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:107:10: ( singleSource -> singleSource | '[' multiSource ']' -> ^( MULTI multiSource ) )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==Identifier) ) {
                alt2=1;
            }
            else if ( (LA2_0==42) ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:107:12: singleSource
                    {
                    pushFollow(FOLLOW_singleSource_in_source226);
                    singleSource13=singleSource();

                    state._fsp--;

                    stream_singleSource.add(singleSource13.getTree());


                    // AST REWRITE
                    // elements: singleSource
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 107:26: -> singleSource
                    {
                        adaptor.addChild(root_0, stream_singleSource.nextTree());

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:6: '[' multiSource ']'
                    {
                    char_literal14=(Token)match(input,42,FOLLOW_42_in_source238);  
                    stream_42.add(char_literal14);

                    pushFollow(FOLLOW_multiSource_in_source240);
                    multiSource15=multiSource();

                    state._fsp--;

                    stream_multiSource.add(multiSource15.getTree());
                    char_literal16=(Token)match(input,43,FOLLOW_43_in_source242);  
                    stream_43.add(char_literal16);



                    // AST REWRITE
                    // elements: multiSource
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 108:26: -> ^( MULTI multiSource )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:29: ^( MULTI multiSource )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(MULTI, "MULTI"), root_1);

                        adaptor.addChild(root_1, stream_multiSource.nextTree());

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
    // $ANTLR end "source"

    public static class sourceEof_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "sourceEof"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:1: sourceEof : source EOF -> source ;
    public final FlumeDeployParser.sourceEof_return sourceEof() throws RecognitionException {
        FlumeDeployParser.sourceEof_return retval = new FlumeDeployParser.sourceEof_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token EOF18=null;
        FlumeDeployParser.source_return source17 = null;


        Object EOF18_tree=null;
        RewriteRuleTokenStream stream_EOF=new RewriteRuleTokenStream(adaptor,"token EOF");
        RewriteRuleSubtreeStream stream_source=new RewriteRuleSubtreeStream(adaptor,"rule source");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:11: ( source EOF -> source )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:14: source EOF
            {
            pushFollow(FOLLOW_source_in_sourceEof260);
            source17=source();

            state._fsp--;

            stream_source.add(source17.getTree());
            EOF18=(Token)match(input,EOF,FOLLOW_EOF_in_sourceEof262);  
            stream_EOF.add(EOF18);



            // AST REWRITE
            // elements: source
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 109:27: -> source
            {
                adaptor.addChild(root_0, stream_source.nextTree());

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
    // $ANTLR end "sourceEof"

    public static class singleSource_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "singleSource"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:1: singleSource : Identifier ( args )? -> ^( SOURCE Identifier ( args )? ) ;
    public final FlumeDeployParser.singleSource_return singleSource() throws RecognitionException {
        FlumeDeployParser.singleSource_return retval = new FlumeDeployParser.singleSource_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token Identifier19=null;
        FlumeDeployParser.args_return args20 = null;


        Object Identifier19_tree=null;
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:14: ( Identifier ( args )? -> ^( SOURCE Identifier ( args )? ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:16: Identifier ( args )?
            {
            Identifier19=(Token)match(input,Identifier,FOLLOW_Identifier_in_singleSource275);  
            stream_Identifier.add(Identifier19);

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:27: ( args )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==56) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:27: args
                    {
                    pushFollow(FOLLOW_args_in_singleSource277);
                    args20=args();

                    state._fsp--;

                    stream_args.add(args20.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: Identifier, args
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 110:33: -> ^( SOURCE Identifier ( args )? )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:36: ^( SOURCE Identifier ( args )? )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(SOURCE, "SOURCE"), root_1);

                adaptor.addChild(root_1, stream_Identifier.nextNode());
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:56: ( args )?
                if ( stream_args.hasNext() ) {
                    adaptor.addChild(root_1, stream_args.nextTree());

                }
                stream_args.reset();

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
    // $ANTLR end "singleSource"

    public static class multiSource_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "multiSource"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:1: multiSource : singleSource ( ',' singleSource )* -> ( singleSource )+ ;
    public final FlumeDeployParser.multiSource_return multiSource() throws RecognitionException {
        FlumeDeployParser.multiSource_return retval = new FlumeDeployParser.multiSource_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal22=null;
        FlumeDeployParser.singleSource_return singleSource21 = null;

        FlumeDeployParser.singleSource_return singleSource23 = null;


        Object char_literal22_tree=null;
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleSubtreeStream stream_singleSource=new RewriteRuleSubtreeStream(adaptor,"rule singleSource");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:13: ( singleSource ( ',' singleSource )* -> ( singleSource )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:15: singleSource ( ',' singleSource )*
            {
            pushFollow(FOLLOW_singleSource_in_multiSource296);
            singleSource21=singleSource();

            state._fsp--;

            stream_singleSource.add(singleSource21.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:28: ( ',' singleSource )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0==44) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:29: ',' singleSource
            	    {
            	    char_literal22=(Token)match(input,44,FOLLOW_44_in_multiSource299);  
            	    stream_44.add(char_literal22);

            	    pushFollow(FOLLOW_singleSource_in_multiSource301);
            	    singleSource23=singleSource();

            	    state._fsp--;

            	    stream_singleSource.add(singleSource23.getTree());

            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);



            // AST REWRITE
            // elements: singleSource
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 111:48: -> ( singleSource )+
            {
                if ( !(stream_singleSource.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_singleSource.hasNext() ) {
                    adaptor.addChild(root_0, stream_singleSource.nextTree());

                }
                stream_singleSource.reset();

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
    // $ANTLR end "multiSource"

    public static class sink_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "sink"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:1: sink : simpleSink -> simpleSink ;
    public final FlumeDeployParser.sink_return sink() throws RecognitionException {
        FlumeDeployParser.sink_return retval = new FlumeDeployParser.sink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        FlumeDeployParser.simpleSink_return simpleSink24 = null;


        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:7: ( simpleSink -> simpleSink )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:9: simpleSink
            {
            pushFollow(FOLLOW_simpleSink_in_sink320);
            simpleSink24=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink24.getTree());


            // AST REWRITE
            // elements: simpleSink
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 114:20: -> simpleSink
            {
                adaptor.addChild(root_0, stream_simpleSink.nextTree());

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
    // $ANTLR end "sink"

    public static class singleSink_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "singleSink"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:1: singleSink : Identifier ( args )? -> ^( SINK Identifier ( args )? ) ;
    public final FlumeDeployParser.singleSink_return singleSink() throws RecognitionException {
        FlumeDeployParser.singleSink_return retval = new FlumeDeployParser.singleSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token Identifier25=null;
        FlumeDeployParser.args_return args26 = null;


        Object Identifier25_tree=null;
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:12: ( Identifier ( args )? -> ^( SINK Identifier ( args )? ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:14: Identifier ( args )?
            {
            Identifier25=(Token)match(input,Identifier,FOLLOW_Identifier_in_singleSink332);  
            stream_Identifier.add(Identifier25);

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:25: ( args )?
            int alt5=2;
            alt5 = dfa5.predict(input);
            switch (alt5) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:25: args
                    {
                    pushFollow(FOLLOW_args_in_singleSink334);
                    args26=args();

                    state._fsp--;

                    stream_args.add(args26.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: args, Identifier
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 116:32: -> ^( SINK Identifier ( args )? )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:35: ^( SINK Identifier ( args )? )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(SINK, "SINK"), root_1);

                adaptor.addChild(root_1, stream_Identifier.nextNode());
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:53: ( args )?
                if ( stream_args.hasNext() ) {
                    adaptor.addChild(root_1, stream_args.nextTree());

                }
                stream_args.reset();

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
    // $ANTLR end "singleSink"

    public static class sinkEof_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "sinkEof"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:118:1: sinkEof : simpleSink EOF ;
    public final FlumeDeployParser.sinkEof_return sinkEof() throws RecognitionException {
        FlumeDeployParser.sinkEof_return retval = new FlumeDeployParser.sinkEof_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token EOF28=null;
        FlumeDeployParser.simpleSink_return simpleSink27 = null;


        Object EOF28_tree=null;

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:118:10: ( simpleSink EOF )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:118:12: simpleSink EOF
            {
            root_0 = (Object)adaptor.nil();

            pushFollow(FOLLOW_simpleSink_in_sinkEof356);
            simpleSink27=simpleSink();

            state._fsp--;

            adaptor.addChild(root_0, simpleSink27.getTree());
            EOF28=(Token)match(input,EOF,FOLLOW_EOF_in_sinkEof358); 
            EOF28_tree = (Object)adaptor.create(EOF28);
            adaptor.addChild(root_0, EOF28_tree);


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
    // $ANTLR end "sinkEof"

    public static class simpleSink_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "simpleSink"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:120:1: simpleSink : ( '[' multiSink ']' -> ^( MULTI multiSink ) | '{' decoratedSink '}' -> ^( DECO decoratedSink ) | '<' failoverSink '>' -> ^( BACKUP failoverSink ) | letSink -> letSink | singleSink -> singleSink | rollSink -> rollSink | failoverChain -> failoverChain );
    public final FlumeDeployParser.simpleSink_return simpleSink() throws RecognitionException {
        FlumeDeployParser.simpleSink_return retval = new FlumeDeployParser.simpleSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal29=null;
        Token char_literal31=null;
        Token char_literal32=null;
        Token char_literal34=null;
        Token char_literal35=null;
        Token char_literal37=null;
        FlumeDeployParser.multiSink_return multiSink30 = null;

        FlumeDeployParser.decoratedSink_return decoratedSink33 = null;

        FlumeDeployParser.failoverSink_return failoverSink36 = null;

        FlumeDeployParser.letSink_return letSink38 = null;

        FlumeDeployParser.singleSink_return singleSink39 = null;

        FlumeDeployParser.rollSink_return rollSink40 = null;

        FlumeDeployParser.failoverChain_return failoverChain41 = null;


        Object char_literal29_tree=null;
        Object char_literal31_tree=null;
        Object char_literal32_tree=null;
        Object char_literal34_tree=null;
        Object char_literal35_tree=null;
        Object char_literal37_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_43=new RewriteRuleTokenStream(adaptor,"token 43");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleSubtreeStream stream_letSink=new RewriteRuleSubtreeStream(adaptor,"rule letSink");
        RewriteRuleSubtreeStream stream_multiSink=new RewriteRuleSubtreeStream(adaptor,"rule multiSink");
        RewriteRuleSubtreeStream stream_failoverChain=new RewriteRuleSubtreeStream(adaptor,"rule failoverChain");
        RewriteRuleSubtreeStream stream_failoverSink=new RewriteRuleSubtreeStream(adaptor,"rule failoverSink");
        RewriteRuleSubtreeStream stream_singleSink=new RewriteRuleSubtreeStream(adaptor,"rule singleSink");
        RewriteRuleSubtreeStream stream_rollSink=new RewriteRuleSubtreeStream(adaptor,"rule rollSink");
        RewriteRuleSubtreeStream stream_decoratedSink=new RewriteRuleSubtreeStream(adaptor,"rule decoratedSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:120:12: ( '[' multiSink ']' -> ^( MULTI multiSink ) | '{' decoratedSink '}' -> ^( DECO decoratedSink ) | '<' failoverSink '>' -> ^( BACKUP failoverSink ) | letSink -> letSink | singleSink -> singleSink | rollSink -> rollSink | failoverChain -> failoverChain )
            int alt6=7;
            switch ( input.LA(1) ) {
            case 42:
                {
                alt6=1;
                }
                break;
            case 45:
                {
                alt6=2;
                }
                break;
            case 47:
                {
                alt6=3;
                }
                break;
            case 51:
                {
                alt6=4;
                }
                break;
            case Identifier:
                {
                alt6=5;
                }
                break;
            case 54:
                {
                alt6=6;
                }
                break;
            case 55:
                {
                alt6=7;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 0, input);

                throw nvae;
            }

            switch (alt6) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:120:14: '[' multiSink ']'
                    {
                    char_literal29=(Token)match(input,42,FOLLOW_42_in_simpleSink366);  
                    stream_42.add(char_literal29);

                    pushFollow(FOLLOW_multiSink_in_simpleSink368);
                    multiSink30=multiSink();

                    state._fsp--;

                    stream_multiSink.add(multiSink30.getTree());
                    char_literal31=(Token)match(input,43,FOLLOW_43_in_simpleSink370);  
                    stream_43.add(char_literal31);



                    // AST REWRITE
                    // elements: multiSink
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 120:34: -> ^( MULTI multiSink )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:120:37: ^( MULTI multiSink )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(MULTI, "MULTI"), root_1);

                        adaptor.addChild(root_1, stream_multiSink.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:121:5: '{' decoratedSink '}'
                    {
                    char_literal32=(Token)match(input,45,FOLLOW_45_in_simpleSink387);  
                    stream_45.add(char_literal32);

                    pushFollow(FOLLOW_decoratedSink_in_simpleSink389);
                    decoratedSink33=decoratedSink();

                    state._fsp--;

                    stream_decoratedSink.add(decoratedSink33.getTree());
                    char_literal34=(Token)match(input,46,FOLLOW_46_in_simpleSink391);  
                    stream_46.add(char_literal34);



                    // AST REWRITE
                    // elements: decoratedSink
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 121:27: -> ^( DECO decoratedSink )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:121:30: ^( DECO decoratedSink )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(DECO, "DECO"), root_1);

                        adaptor.addChild(root_1, stream_decoratedSink.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:122:5: '<' failoverSink '>'
                    {
                    char_literal35=(Token)match(input,47,FOLLOW_47_in_simpleSink405);  
                    stream_47.add(char_literal35);

                    pushFollow(FOLLOW_failoverSink_in_simpleSink407);
                    failoverSink36=failoverSink();

                    state._fsp--;

                    stream_failoverSink.add(failoverSink36.getTree());
                    char_literal37=(Token)match(input,48,FOLLOW_48_in_simpleSink409);  
                    stream_48.add(char_literal37);



                    // AST REWRITE
                    // elements: failoverSink
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 122:26: -> ^( BACKUP failoverSink )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:122:29: ^( BACKUP failoverSink )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(BACKUP, "BACKUP"), root_1);

                        adaptor.addChild(root_1, stream_failoverSink.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 4 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:123:7: letSink
                    {
                    pushFollow(FOLLOW_letSink_in_simpleSink425);
                    letSink38=letSink();

                    state._fsp--;

                    stream_letSink.add(letSink38.getTree());


                    // AST REWRITE
                    // elements: letSink
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 123:31: -> letSink
                    {
                        adaptor.addChild(root_0, stream_letSink.nextTree());

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 5 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:124:5: singleSink
                    {
                    pushFollow(FOLLOW_singleSink_in_simpleSink451);
                    singleSink39=singleSink();

                    state._fsp--;

                    stream_singleSink.add(singleSink39.getTree());


                    // AST REWRITE
                    // elements: singleSink
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 124:29: -> singleSink
                    {
                        adaptor.addChild(root_0, stream_singleSink.nextTree());

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 6 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:125:13: rollSink
                    {
                    pushFollow(FOLLOW_rollSink_in_simpleSink483);
                    rollSink40=rollSink();

                    state._fsp--;

                    stream_rollSink.add(rollSink40.getTree());


                    // AST REWRITE
                    // elements: rollSink
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 125:37: -> rollSink
                    {
                        adaptor.addChild(root_0, stream_rollSink.nextTree());

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 7 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:126:13: failoverChain
                    {
                    pushFollow(FOLLOW_failoverChain_in_simpleSink516);
                    failoverChain41=failoverChain();

                    state._fsp--;

                    stream_failoverChain.add(failoverChain41.getTree());


                    // AST REWRITE
                    // elements: failoverChain
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 126:37: -> failoverChain
                    {
                        adaptor.addChild(root_0, stream_failoverChain.nextTree());

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
    // $ANTLR end "simpleSink"

    public static class decoratedSink_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "decoratedSink"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:129:1: decoratedSink : singleSink '=>' sink -> singleSink sink ;
    public final FlumeDeployParser.decoratedSink_return decoratedSink() throws RecognitionException {
        FlumeDeployParser.decoratedSink_return retval = new FlumeDeployParser.decoratedSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token string_literal43=null;
        FlumeDeployParser.singleSink_return singleSink42 = null;

        FlumeDeployParser.sink_return sink44 = null;


        Object string_literal43_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleSubtreeStream stream_sink=new RewriteRuleSubtreeStream(adaptor,"rule sink");
        RewriteRuleSubtreeStream stream_singleSink=new RewriteRuleSubtreeStream(adaptor,"rule singleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:129:17: ( singleSink '=>' sink -> singleSink sink )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:129:20: singleSink '=>' sink
            {
            pushFollow(FOLLOW_singleSink_in_decoratedSink550);
            singleSink42=singleSink();

            state._fsp--;

            stream_singleSink.add(singleSink42.getTree());
            string_literal43=(Token)match(input,49,FOLLOW_49_in_decoratedSink552);  
            stream_49.add(string_literal43);

            pushFollow(FOLLOW_sink_in_decoratedSink554);
            sink44=sink();

            state._fsp--;

            stream_sink.add(sink44.getTree());


            // AST REWRITE
            // elements: sink, singleSink
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 129:44: -> singleSink sink
            {
                adaptor.addChild(root_0, stream_singleSink.nextTree());
                adaptor.addChild(root_0, stream_sink.nextTree());

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
    // $ANTLR end "decoratedSink"

    public static class multiSink_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "multiSink"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:1: multiSink : simpleSink ( ',' simpleSink )* -> ( simpleSink )* ;
    public final FlumeDeployParser.multiSink_return multiSink() throws RecognitionException {
        FlumeDeployParser.multiSink_return retval = new FlumeDeployParser.multiSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal46=null;
        FlumeDeployParser.simpleSink_return simpleSink45 = null;

        FlumeDeployParser.simpleSink_return simpleSink47 = null;


        Object char_literal46_tree=null;
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:17: ( simpleSink ( ',' simpleSink )* -> ( simpleSink )* )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:20: simpleSink ( ',' simpleSink )*
            {
            pushFollow(FOLLOW_simpleSink_in_multiSink577);
            simpleSink45=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink45.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:31: ( ',' simpleSink )*
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( (LA7_0==44) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:32: ',' simpleSink
            	    {
            	    char_literal46=(Token)match(input,44,FOLLOW_44_in_multiSink580);  
            	    stream_44.add(char_literal46);

            	    pushFollow(FOLLOW_simpleSink_in_multiSink582);
            	    simpleSink47=simpleSink();

            	    state._fsp--;

            	    stream_simpleSink.add(simpleSink47.getTree());

            	    }
            	    break;

            	default :
            	    break loop7;
                }
            } while (true);



            // AST REWRITE
            // elements: simpleSink
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 130:50: -> ( simpleSink )*
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:53: ( simpleSink )*
                while ( stream_simpleSink.hasNext() ) {
                    adaptor.addChild(root_0, stream_simpleSink.nextTree());

                }
                stream_simpleSink.reset();

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
    // $ANTLR end "multiSink"

    public static class failoverSink_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "failoverSink"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:1: failoverSink : simpleSink ( '?' simpleSink )+ -> ( simpleSink )+ ;
    public final FlumeDeployParser.failoverSink_return failoverSink() throws RecognitionException {
        FlumeDeployParser.failoverSink_return retval = new FlumeDeployParser.failoverSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal49=null;
        FlumeDeployParser.simpleSink_return simpleSink48 = null;

        FlumeDeployParser.simpleSink_return simpleSink50 = null;


        Object char_literal49_tree=null;
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:17: ( simpleSink ( '?' simpleSink )+ -> ( simpleSink )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:20: simpleSink ( '?' simpleSink )+
            {
            pushFollow(FOLLOW_simpleSink_in_failoverSink602);
            simpleSink48=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink48.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:31: ( '?' simpleSink )+
            int cnt8=0;
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==50) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:32: '?' simpleSink
            	    {
            	    char_literal49=(Token)match(input,50,FOLLOW_50_in_failoverSink605);  
            	    stream_50.add(char_literal49);

            	    pushFollow(FOLLOW_simpleSink_in_failoverSink607);
            	    simpleSink50=simpleSink();

            	    state._fsp--;

            	    stream_simpleSink.add(simpleSink50.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt8 >= 1 ) break loop8;
                        EarlyExitException eee =
                            new EarlyExitException(8, input);
                        throw eee;
                }
                cnt8++;
            } while (true);



            // AST REWRITE
            // elements: simpleSink
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 131:49: -> ( simpleSink )+
            {
                if ( !(stream_simpleSink.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_simpleSink.hasNext() ) {
                    adaptor.addChild(root_0, stream_simpleSink.nextTree());

                }
                stream_simpleSink.reset();

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
    // $ANTLR end "failoverSink"

    public static class letSink_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "letSink"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:1: letSink : 'let' Identifier ':=' simpleSink 'in' simpleSink -> ^( LET Identifier ( simpleSink )+ ) ;
    public final FlumeDeployParser.letSink_return letSink() throws RecognitionException {
        FlumeDeployParser.letSink_return retval = new FlumeDeployParser.letSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token string_literal51=null;
        Token Identifier52=null;
        Token string_literal53=null;
        Token string_literal55=null;
        FlumeDeployParser.simpleSink_return simpleSink54 = null;

        FlumeDeployParser.simpleSink_return simpleSink56 = null;


        Object string_literal51_tree=null;
        Object Identifier52_tree=null;
        Object string_literal53_tree=null;
        Object string_literal55_tree=null;
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleTokenStream stream_52=new RewriteRuleTokenStream(adaptor,"token 52");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:17: ( 'let' Identifier ':=' simpleSink 'in' simpleSink -> ^( LET Identifier ( simpleSink )+ ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:20: 'let' Identifier ':=' simpleSink 'in' simpleSink
            {
            string_literal51=(Token)match(input,51,FOLLOW_51_in_letSink631);  
            stream_51.add(string_literal51);

            Identifier52=(Token)match(input,Identifier,FOLLOW_Identifier_in_letSink633);  
            stream_Identifier.add(Identifier52);

            string_literal53=(Token)match(input,52,FOLLOW_52_in_letSink635);  
            stream_52.add(string_literal53);

            pushFollow(FOLLOW_simpleSink_in_letSink637);
            simpleSink54=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink54.getTree());
            string_literal55=(Token)match(input,53,FOLLOW_53_in_letSink639);  
            stream_53.add(string_literal55);

            pushFollow(FOLLOW_simpleSink_in_letSink641);
            simpleSink56=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink56.getTree());


            // AST REWRITE
            // elements: Identifier, simpleSink
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 133:35: -> ^( LET Identifier ( simpleSink )+ )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:133:38: ^( LET Identifier ( simpleSink )+ )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(LET, "LET"), root_1);

                adaptor.addChild(root_1, stream_Identifier.nextNode());
                if ( !(stream_simpleSink.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_simpleSink.hasNext() ) {
                    adaptor.addChild(root_1, stream_simpleSink.nextTree());

                }
                stream_simpleSink.reset();

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
    // $ANTLR end "letSink"

    public static class rollSink_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "rollSink"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:134:1: rollSink : 'roll' args '{' simpleSink '}' -> ^( ROLL simpleSink args ) ;
    public final FlumeDeployParser.rollSink_return rollSink() throws RecognitionException {
        FlumeDeployParser.rollSink_return retval = new FlumeDeployParser.rollSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token string_literal57=null;
        Token char_literal59=null;
        Token char_literal61=null;
        FlumeDeployParser.args_return args58 = null;

        FlumeDeployParser.simpleSink_return simpleSink60 = null;


        Object string_literal57_tree=null;
        Object char_literal59_tree=null;
        Object char_literal61_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleTokenStream stream_54=new RewriteRuleTokenStream(adaptor,"token 54");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:134:17: ( 'roll' args '{' simpleSink '}' -> ^( ROLL simpleSink args ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:134:20: 'roll' args '{' simpleSink '}'
            {
            string_literal57=(Token)match(input,54,FOLLOW_54_in_rollSink702);  
            stream_54.add(string_literal57);

            pushFollow(FOLLOW_args_in_rollSink704);
            args58=args();

            state._fsp--;

            stream_args.add(args58.getTree());
            char_literal59=(Token)match(input,45,FOLLOW_45_in_rollSink706);  
            stream_45.add(char_literal59);

            pushFollow(FOLLOW_simpleSink_in_rollSink708);
            simpleSink60=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink60.getTree());
            char_literal61=(Token)match(input,46,FOLLOW_46_in_rollSink710);  
            stream_46.add(char_literal61);



            // AST REWRITE
            // elements: simpleSink, args
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 135:35: -> ^( ROLL simpleSink args )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:135:38: ^( ROLL simpleSink args )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(ROLL, "ROLL"), root_1);

                adaptor.addChild(root_1, stream_simpleSink.nextTree());
                adaptor.addChild(root_1, stream_args.nextTree());

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
    // $ANTLR end "rollSink"

    public static class failoverChain_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "failoverChain"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:136:1: failoverChain : 'failchain' args '{' simpleSink '}' -> ^( FAILCHAIN simpleSink args ) ;
    public final FlumeDeployParser.failoverChain_return failoverChain() throws RecognitionException {
        FlumeDeployParser.failoverChain_return retval = new FlumeDeployParser.failoverChain_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token string_literal62=null;
        Token char_literal64=null;
        Token char_literal66=null;
        FlumeDeployParser.args_return args63 = null;

        FlumeDeployParser.simpleSink_return simpleSink65 = null;


        Object string_literal62_tree=null;
        Object char_literal64_tree=null;
        Object char_literal66_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:136:17: ( 'failchain' args '{' simpleSink '}' -> ^( FAILCHAIN simpleSink args ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:136:20: 'failchain' args '{' simpleSink '}'
            {
            string_literal62=(Token)match(input,55,FOLLOW_55_in_failoverChain764);  
            stream_55.add(string_literal62);

            pushFollow(FOLLOW_args_in_failoverChain766);
            args63=args();

            state._fsp--;

            stream_args.add(args63.getTree());
            char_literal64=(Token)match(input,45,FOLLOW_45_in_failoverChain768);  
            stream_45.add(char_literal64);

            pushFollow(FOLLOW_simpleSink_in_failoverChain770);
            simpleSink65=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink65.getTree());
            char_literal66=(Token)match(input,46,FOLLOW_46_in_failoverChain772);  
            stream_46.add(char_literal66);



            // AST REWRITE
            // elements: args, simpleSink
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 137:35: -> ^( FAILCHAIN simpleSink args )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:137:38: ^( FAILCHAIN simpleSink args )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(FAILCHAIN, "FAILCHAIN"), root_1);

                adaptor.addChild(root_1, stream_simpleSink.nextTree());
                adaptor.addChild(root_1, stream_args.nextTree());

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
    // $ANTLR end "failoverChain"

    public static class arglist_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arglist"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:1: arglist : literal ( ',' literal )* -> ( literal )+ ;
    public final FlumeDeployParser.arglist_return arglist() throws RecognitionException {
        FlumeDeployParser.arglist_return retval = new FlumeDeployParser.arglist_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal68=null;
        FlumeDeployParser.literal_return literal67 = null;

        FlumeDeployParser.literal_return literal69 = null;


        Object char_literal68_tree=null;
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleSubtreeStream stream_literal=new RewriteRuleSubtreeStream(adaptor,"rule literal");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:9: ( literal ( ',' literal )* -> ( literal )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:11: literal ( ',' literal )*
            {
            pushFollow(FOLLOW_literal_in_arglist824);
            literal67=literal();

            state._fsp--;

            stream_literal.add(literal67.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:19: ( ',' literal )*
            loop9:
            do {
                int alt9=2;
                int LA9_0 = input.LA(1);

                if ( (LA9_0==44) ) {
                    alt9=1;
                }


                switch (alt9) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:20: ',' literal
            	    {
            	    char_literal68=(Token)match(input,44,FOLLOW_44_in_arglist827);  
            	    stream_44.add(char_literal68);

            	    pushFollow(FOLLOW_literal_in_arglist829);
            	    literal69=literal();

            	    state._fsp--;

            	    stream_literal.add(literal69.getTree());

            	    }
            	    break;

            	default :
            	    break loop9;
                }
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
            // 139:34: -> ( literal )+
            {
                if ( !(stream_literal.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_literal.hasNext() ) {
                    adaptor.addChild(root_0, stream_literal.nextTree());

                }
                stream_literal.reset();

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
    // $ANTLR end "arglist"

    public static class args_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "args"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:1: args : '(' ( arglist )? ')' -> ( arglist )? ;
    public final FlumeDeployParser.args_return args() throws RecognitionException {
        FlumeDeployParser.args_return retval = new FlumeDeployParser.args_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal70=null;
        Token char_literal72=null;
        FlumeDeployParser.arglist_return arglist71 = null;


        Object char_literal70_tree=null;
        Object char_literal72_tree=null;
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleSubtreeStream stream_arglist=new RewriteRuleSubtreeStream(adaptor,"rule arglist");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:7: ( '(' ( arglist )? ')' -> ( arglist )? )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:9: '(' ( arglist )? ')'
            {
            char_literal70=(Token)match(input,56,FOLLOW_56_in_args845);  
            stream_56.add(char_literal70);

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:13: ( arglist )?
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( ((LA10_0>=StringLiteral && LA10_0<=DecimalLiteral)||(LA10_0>=58 && LA10_0<=59)) ) {
                alt10=1;
            }
            switch (alt10) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:13: arglist
                    {
                    pushFollow(FOLLOW_arglist_in_args847);
                    arglist71=arglist();

                    state._fsp--;

                    stream_arglist.add(arglist71.getTree());

                    }
                    break;

            }

            char_literal72=(Token)match(input,57,FOLLOW_57_in_args850);  
            stream_57.add(char_literal72);



            // AST REWRITE
            // elements: arglist
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 140:26: -> ( arglist )?
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:29: ( arglist )?
                if ( stream_arglist.hasNext() ) {
                    adaptor.addChild(root_0, stream_arglist.nextTree());

                }
                stream_arglist.reset();

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
    // $ANTLR end "args"

    public static class literal_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "literal"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:144:1: literal : ( integerLiteral | StringLiteral -> ^( STRING StringLiteral ) | booleanLiteral | FloatingPointLiteral -> ^( FLOAT FloatingPointLiteral ) );
    public final FlumeDeployParser.literal_return literal() throws RecognitionException {
        FlumeDeployParser.literal_return retval = new FlumeDeployParser.literal_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token StringLiteral74=null;
        Token FloatingPointLiteral76=null;
        FlumeDeployParser.integerLiteral_return integerLiteral73 = null;

        FlumeDeployParser.booleanLiteral_return booleanLiteral75 = null;


        Object StringLiteral74_tree=null;
        Object FloatingPointLiteral76_tree=null;
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleTokenStream stream_FloatingPointLiteral=new RewriteRuleTokenStream(adaptor,"token FloatingPointLiteral");

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:145:5: ( integerLiteral | StringLiteral -> ^( STRING StringLiteral ) | booleanLiteral | FloatingPointLiteral -> ^( FLOAT FloatingPointLiteral ) )
            int alt11=4;
            switch ( input.LA(1) ) {
            case HexLiteral:
            case OctalLiteral:
            case DecimalLiteral:
                {
                alt11=1;
                }
                break;
            case StringLiteral:
                {
                alt11=2;
                }
                break;
            case 58:
            case 59:
                {
                alt11=3;
                }
                break;
            case FloatingPointLiteral:
                {
                alt11=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;
            }

            switch (alt11) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:145:9: integerLiteral
                    {
                    root_0 = (Object)adaptor.nil();

                    pushFollow(FOLLOW_integerLiteral_in_literal873);
                    integerLiteral73=integerLiteral();

                    state._fsp--;

                    adaptor.addChild(root_0, integerLiteral73.getTree());

                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:146:9: StringLiteral
                    {
                    StringLiteral74=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_literal883);  
                    stream_StringLiteral.add(StringLiteral74);



                    // AST REWRITE
                    // elements: StringLiteral
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 146:24: -> ^( STRING StringLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:146:27: ^( STRING StringLiteral )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(STRING, "STRING"), root_1);

                        adaptor.addChild(root_1, stream_StringLiteral.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:147:9: booleanLiteral
                    {
                    root_0 = (Object)adaptor.nil();

                    pushFollow(FOLLOW_booleanLiteral_in_literal902);
                    booleanLiteral75=booleanLiteral();

                    state._fsp--;

                    adaptor.addChild(root_0, booleanLiteral75.getTree());

                    }
                    break;
                case 4 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:148:9: FloatingPointLiteral
                    {
                    FloatingPointLiteral76=(Token)match(input,FloatingPointLiteral,FOLLOW_FloatingPointLiteral_in_literal912);  
                    stream_FloatingPointLiteral.add(FloatingPointLiteral76);



                    // AST REWRITE
                    // elements: FloatingPointLiteral
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 148:30: -> ^( FLOAT FloatingPointLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:148:33: ^( FLOAT FloatingPointLiteral )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(FLOAT, "FLOAT"), root_1);

                        adaptor.addChild(root_1, stream_FloatingPointLiteral.nextNode());

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

    public static class integerLiteral_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "integerLiteral"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:150:1: integerLiteral : ( HexLiteral -> ^( HEX HexLiteral ) | OctalLiteral -> ^( OCT OctalLiteral ) | DecimalLiteral -> ^( DEC DecimalLiteral ) );
    public final FlumeDeployParser.integerLiteral_return integerLiteral() throws RecognitionException {
        FlumeDeployParser.integerLiteral_return retval = new FlumeDeployParser.integerLiteral_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token HexLiteral77=null;
        Token OctalLiteral78=null;
        Token DecimalLiteral79=null;

        Object HexLiteral77_tree=null;
        Object OctalLiteral78_tree=null;
        Object DecimalLiteral79_tree=null;
        RewriteRuleTokenStream stream_HexLiteral=new RewriteRuleTokenStream(adaptor,"token HexLiteral");
        RewriteRuleTokenStream stream_DecimalLiteral=new RewriteRuleTokenStream(adaptor,"token DecimalLiteral");
        RewriteRuleTokenStream stream_OctalLiteral=new RewriteRuleTokenStream(adaptor,"token OctalLiteral");

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:151:5: ( HexLiteral -> ^( HEX HexLiteral ) | OctalLiteral -> ^( OCT OctalLiteral ) | DecimalLiteral -> ^( DEC DecimalLiteral ) )
            int alt12=3;
            switch ( input.LA(1) ) {
            case HexLiteral:
                {
                alt12=1;
                }
                break;
            case OctalLiteral:
                {
                alt12=2;
                }
                break;
            case DecimalLiteral:
                {
                alt12=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }

            switch (alt12) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:151:9: HexLiteral
                    {
                    HexLiteral77=(Token)match(input,HexLiteral,FOLLOW_HexLiteral_in_integerLiteral938);  
                    stream_HexLiteral.add(HexLiteral77);



                    // AST REWRITE
                    // elements: HexLiteral
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 151:21: -> ^( HEX HexLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:151:24: ^( HEX HexLiteral )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(HEX, "HEX"), root_1);

                        adaptor.addChild(root_1, stream_HexLiteral.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:152:9: OctalLiteral
                    {
                    OctalLiteral78=(Token)match(input,OctalLiteral,FOLLOW_OctalLiteral_in_integerLiteral957);  
                    stream_OctalLiteral.add(OctalLiteral78);



                    // AST REWRITE
                    // elements: OctalLiteral
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 152:23: -> ^( OCT OctalLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:152:26: ^( OCT OctalLiteral )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(OCT, "OCT"), root_1);

                        adaptor.addChild(root_1, stream_OctalLiteral.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:153:9: DecimalLiteral
                    {
                    DecimalLiteral79=(Token)match(input,DecimalLiteral,FOLLOW_DecimalLiteral_in_integerLiteral976);  
                    stream_DecimalLiteral.add(DecimalLiteral79);



                    // AST REWRITE
                    // elements: DecimalLiteral
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 153:25: -> ^( DEC DecimalLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:153:28: ^( DEC DecimalLiteral )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(DEC, "DEC"), root_1);

                        adaptor.addChild(root_1, stream_DecimalLiteral.nextNode());

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
    // $ANTLR end "integerLiteral"

    public static class booleanLiteral_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "booleanLiteral"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:156:1: booleanLiteral : ( 'true' -> ^( BOOL 'true' ) | 'false' -> ^( BOOL 'false' ) );
    public final FlumeDeployParser.booleanLiteral_return booleanLiteral() throws RecognitionException {
        FlumeDeployParser.booleanLiteral_return retval = new FlumeDeployParser.booleanLiteral_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token string_literal80=null;
        Token string_literal81=null;

        Object string_literal80_tree=null;
        Object string_literal81_tree=null;
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:157:5: ( 'true' -> ^( BOOL 'true' ) | 'false' -> ^( BOOL 'false' ) )
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( (LA13_0==58) ) {
                alt13=1;
            }
            else if ( (LA13_0==59) ) {
                alt13=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 13, 0, input);

                throw nvae;
            }
            switch (alt13) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:157:9: 'true'
                    {
                    string_literal80=(Token)match(input,58,FOLLOW_58_in_booleanLiteral1008);  
                    stream_58.add(string_literal80);



                    // AST REWRITE
                    // elements: 58
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 157:18: -> ^( BOOL 'true' )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:157:21: ^( BOOL 'true' )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(BOOL, "BOOL"), root_1);

                        adaptor.addChild(root_1, stream_58.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:158:9: 'false'
                    {
                    string_literal81=(Token)match(input,59,FOLLOW_59_in_booleanLiteral1028);  
                    stream_59.add(string_literal81);



                    // AST REWRITE
                    // elements: 59
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 158:18: -> ^( BOOL 'false' )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:158:21: ^( BOOL 'false' )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(BOOL, "BOOL"), root_1);

                        adaptor.addChild(root_1, stream_59.nextNode());

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
    // $ANTLR end "booleanLiteral"

    // Delegated rules


    protected DFA5 dfa5 = new DFA5(this);
    static final String DFA5_eotS =
        "\13\uffff";
    static final String DFA5_eofS =
        "\1\2\12\uffff";
    static final String DFA5_minS =
        "\1\51\12\uffff";
    static final String DFA5_maxS =
        "\1\70\12\uffff";
    static final String DFA5_acceptS =
        "\1\uffff\1\1\1\2\10\uffff";
    static final String DFA5_specialS =
        "\13\uffff}>";
    static final String[] DFA5_transitionS = {
            "\1\2\1\uffff\2\2\1\uffff\1\2\1\uffff\3\2\2\uffff\1\2\2\uffff"+
            "\1\1",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA5_eot = DFA.unpackEncodedString(DFA5_eotS);
    static final short[] DFA5_eof = DFA.unpackEncodedString(DFA5_eofS);
    static final char[] DFA5_min = DFA.unpackEncodedStringToUnsignedChars(DFA5_minS);
    static final char[] DFA5_max = DFA.unpackEncodedStringToUnsignedChars(DFA5_maxS);
    static final short[] DFA5_accept = DFA.unpackEncodedString(DFA5_acceptS);
    static final short[] DFA5_special = DFA.unpackEncodedString(DFA5_specialS);
    static final short[][] DFA5_transition;

    static {
        int numStates = DFA5_transitionS.length;
        DFA5_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA5_transition[i] = DFA.unpackEncodedString(DFA5_transitionS[i]);
        }
    }

    class DFA5 extends DFA {

        public DFA5(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 5;
            this.eot = DFA5_eot;
            this.eof = DFA5_eof;
            this.min = DFA5_min;
            this.max = DFA5_max;
            this.accept = DFA5_accept;
            this.special = DFA5_special;
            this.transition = DFA5_transition;
        }
        public String getDescription() {
            return "116:25: ( args )?";
        }
    }
 

    public static final BitSet FOLLOW_def_in_deflist140 = new BitSet(new long[]{0x0000000000300000L});
    public static final BitSet FOLLOW_EOF_in_deflist143 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_host_in_def153 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_39_in_def155 = new BitSet(new long[]{0x0000040000100000L});
    public static final BitSet FOLLOW_source_in_def157 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_40_in_def159 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_sink_in_def161 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_41_in_def164 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_host0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_source_in_connection200 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_40_in_connection202 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_sink_in_connection204 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSource_in_source226 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_42_in_source238 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_multiSource_in_source240 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_43_in_source242 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_source_in_sourceEof260 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_sourceEof262 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_singleSource275 = new BitSet(new long[]{0x0100000000000002L});
    public static final BitSet FOLLOW_args_in_singleSource277 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSource_in_multiSource296 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_44_in_multiSource299 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_singleSource_in_multiSource301 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_simpleSink_in_sink320 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_singleSink332 = new BitSet(new long[]{0x0100000000000002L});
    public static final BitSet FOLLOW_args_in_singleSink334 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_simpleSink_in_sinkEof356 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_sinkEof358 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_42_in_simpleSink366 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_multiSink_in_simpleSink368 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_43_in_simpleSink370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_45_in_simpleSink387 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_decoratedSink_in_simpleSink389 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_simpleSink391 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_47_in_simpleSink405 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_failoverSink_in_simpleSink407 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_simpleSink409 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_letSink_in_simpleSink425 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSink_in_simpleSink451 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rollSink_in_simpleSink483 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_failoverChain_in_simpleSink516 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSink_in_decoratedSink550 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_decoratedSink552 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_sink_in_decoratedSink554 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_simpleSink_in_multiSink577 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_44_in_multiSink580 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_simpleSink_in_multiSink582 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_simpleSink_in_failoverSink602 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_failoverSink605 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_simpleSink_in_failoverSink607 = new BitSet(new long[]{0x0004000000000002L});
    public static final BitSet FOLLOW_51_in_letSink631 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_Identifier_in_letSink633 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_52_in_letSink635 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_simpleSink_in_letSink637 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_letSink639 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_simpleSink_in_letSink641 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_54_in_rollSink702 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_args_in_rollSink704 = new BitSet(new long[]{0x0000200000000000L});
    public static final BitSet FOLLOW_45_in_rollSink706 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_simpleSink_in_rollSink708 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_rollSink710 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_failoverChain764 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_args_in_failoverChain766 = new BitSet(new long[]{0x0000200000000000L});
    public static final BitSet FOLLOW_45_in_failoverChain768 = new BitSet(new long[]{0x00C8A40000100000L});
    public static final BitSet FOLLOW_simpleSink_in_failoverChain770 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_failoverChain772 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_arglist824 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_44_in_arglist827 = new BitSet(new long[]{0x0C00000007C00000L});
    public static final BitSet FOLLOW_literal_in_arglist829 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_56_in_args845 = new BitSet(new long[]{0x0E00000007C00000L});
    public static final BitSet FOLLOW_arglist_in_args847 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_args850 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_integerLiteral_in_literal873 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_literal883 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_booleanLiteral_in_literal902 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FloatingPointLiteral_in_literal912 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HexLiteral_in_integerLiteral938 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_OctalLiteral_in_integerLiteral957 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DecimalLiteral_in_integerLiteral976 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_booleanLiteral1008 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_59_in_booleanLiteral1028 = new BitSet(new long[]{0x0000000000000002L});

}