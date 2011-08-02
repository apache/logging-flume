// $ANTLR 3.1.3 Mar 18, 2009 10:09:25 /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g 2010-12-20 18:36:04

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
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "NODE", "BLANK", "SINK", "BACKUP", "LET", "ROLL", "FAILCHAIN", "DECO", "SOURCE", "MULTI", "HEX", "OCT", "DEC", "STRING", "BOOL", "FLOAT", "KWARG", "Identifier", "IPLiteral", "StringLiteral", "FloatingPointLiteral", "HexLiteral", "OctalLiteral", "DecimalLiteral", "HexDigit", "IntegerTypeSuffix", "EscapeSequence", "UnicodeEscape", "OctalEscape", "Letter", "JavaIDDigit", "Exponent", "FloatTypeSuffix", "WS", "COMMENT", "LINE_COMMENT", "':'", "'|'", "';'", "'['", "']'", "','", "'{'", "'}'", "'<'", "'>'", "'=>'", "'?'", "'let'", "':='", "'in'", "'roll'", "'failchain'", "'('", "')'", "'='", "'true'", "'false'"
    };
    public static final int DEC=16;
    public static final int FloatTypeSuffix=36;
    public static final int OctalLiteral=26;
    public static final int Exponent=35;
    public static final int SOURCE=12;
    public static final int FLOAT=19;
    public static final int MULTI=13;
    public static final int T__61=61;
    public static final int EOF=-1;
    public static final int T__60=60;
    public static final int HexDigit=28;
    public static final int SINK=6;
    public static final int Identifier=21;
    public static final int T__55=55;
    public static final int T__56=56;
    public static final int T__57=57;
    public static final int T__58=58;
    public static final int T__51=51;
    public static final int T__52=52;
    public static final int T__53=53;
    public static final int T__54=54;
    public static final int IPLiteral=22;
    public static final int HEX=14;
    public static final int T__59=59;
    public static final int COMMENT=38;
    public static final int T__50=50;
    public static final int T__42=42;
    public static final int HexLiteral=25;
    public static final int T__43=43;
    public static final int T__40=40;
    public static final int FAILCHAIN=10;
    public static final int T__41=41;
    public static final int T__46=46;
    public static final int T__47=47;
    public static final int T__44=44;
    public static final int NODE=4;
    public static final int T__45=45;
    public static final int LINE_COMMENT=39;
    public static final int IntegerTypeSuffix=29;
    public static final int T__48=48;
    public static final int T__49=49;
    public static final int ROLL=9;
    public static final int BLANK=5;
    public static final int BOOL=18;
    public static final int KWARG=20;
    public static final int DecimalLiteral=27;
    public static final int BACKUP=7;
    public static final int StringLiteral=23;
    public static final int OCT=15;
    public static final int WS=37;
    public static final int UnicodeEscape=31;
    public static final int DECO=11;
    public static final int FloatingPointLiteral=24;
    public static final int JavaIDDigit=34;
    public static final int Letter=33;
    public static final int OctalEscape=32;
    public static final int EscapeSequence=30;
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:97:1: deflist : ( def )* EOF ;
    public final FlumeDeployParser.deflist_return deflist() throws RecognitionException {
        FlumeDeployParser.deflist_return retval = new FlumeDeployParser.deflist_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token EOF2=null;
        FlumeDeployParser.def_return def1 = null;


        Object EOF2_tree=null;

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:97:9: ( ( def )* EOF )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:97:11: ( def )* EOF
            {
            root_0 = (Object)adaptor.nil();

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:97:11: ( def )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>=Identifier && LA1_0<=IPLiteral)) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:97:11: def
            	    {
            	    pushFollow(FOLLOW_def_in_deflist145);
            	    def1=def();

            	    state._fsp--;

            	    adaptor.addChild(root_0, def1.getTree());

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            EOF2=(Token)match(input,EOF,FOLLOW_EOF_in_deflist148); 
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:99:1: def : host ':' source '|' sink ';' -> ^( NODE host source sink ) ;
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
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleSubtreeStream stream_host=new RewriteRuleSubtreeStream(adaptor,"rule host");
        RewriteRuleSubtreeStream stream_source=new RewriteRuleSubtreeStream(adaptor,"rule source");
        RewriteRuleSubtreeStream stream_sink=new RewriteRuleSubtreeStream(adaptor,"rule sink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:99:5: ( host ':' source '|' sink ';' -> ^( NODE host source sink ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:99:7: host ':' source '|' sink ';'
            {
            pushFollow(FOLLOW_host_in_def158);
            host3=host();

            state._fsp--;

            stream_host.add(host3.getTree());
            char_literal4=(Token)match(input,40,FOLLOW_40_in_def160);  
            stream_40.add(char_literal4);

            pushFollow(FOLLOW_source_in_def162);
            source5=source();

            state._fsp--;

            stream_source.add(source5.getTree());
            char_literal6=(Token)match(input,41,FOLLOW_41_in_def164);  
            stream_41.add(char_literal6);

            pushFollow(FOLLOW_sink_in_def166);
            sink7=sink();

            state._fsp--;

            stream_sink.add(sink7.getTree());
            char_literal8=(Token)match(input,42,FOLLOW_42_in_def169);  
            stream_42.add(char_literal8);



            // AST REWRITE
            // elements: sink, host, source
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 99:37: -> ^( NODE host source sink )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:99:40: ^( NODE host source sink )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:101:1: host : ( Identifier | IPLiteral );
    public final FlumeDeployParser.host_return host() throws RecognitionException {
        FlumeDeployParser.host_return retval = new FlumeDeployParser.host_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token set9=null;

        Object set9_tree=null;

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:101:5: ( Identifier | IPLiteral )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:105:1: connection : source '|' sink -> ^( NODE BLANK source sink ) ;
    public final FlumeDeployParser.connection_return connection() throws RecognitionException {
        FlumeDeployParser.connection_return retval = new FlumeDeployParser.connection_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal11=null;
        FlumeDeployParser.source_return source10 = null;

        FlumeDeployParser.sink_return sink12 = null;


        Object char_literal11_tree=null;
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_source=new RewriteRuleSubtreeStream(adaptor,"rule source");
        RewriteRuleSubtreeStream stream_sink=new RewriteRuleSubtreeStream(adaptor,"rule sink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:106:2: ( source '|' sink -> ^( NODE BLANK source sink ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:106:5: source '|' sink
            {
            pushFollow(FOLLOW_source_in_connection205);
            source10=source();

            state._fsp--;

            stream_source.add(source10.getTree());
            char_literal11=(Token)match(input,41,FOLLOW_41_in_connection207);  
            stream_41.add(char_literal11);

            pushFollow(FOLLOW_sink_in_connection209);
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
            // 106:21: -> ^( NODE BLANK source sink )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:106:24: ^( NODE BLANK source sink )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:1: source : ( singleSource -> singleSource | '[' multiSource ']' -> ^( MULTI multiSource ) );
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
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleSubtreeStream stream_multiSource=new RewriteRuleSubtreeStream(adaptor,"rule multiSource");
        RewriteRuleSubtreeStream stream_singleSource=new RewriteRuleSubtreeStream(adaptor,"rule singleSource");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:10: ( singleSource -> singleSource | '[' multiSource ']' -> ^( MULTI multiSource ) )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==Identifier) ) {
                alt2=1;
            }
            else if ( (LA2_0==43) ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:12: singleSource
                    {
                    pushFollow(FOLLOW_singleSource_in_source231);
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
                    // 108:26: -> singleSource
                    {
                        adaptor.addChild(root_0, stream_singleSource.nextTree());

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:6: '[' multiSource ']'
                    {
                    char_literal14=(Token)match(input,43,FOLLOW_43_in_source243);  
                    stream_43.add(char_literal14);

                    pushFollow(FOLLOW_multiSource_in_source245);
                    multiSource15=multiSource();

                    state._fsp--;

                    stream_multiSource.add(multiSource15.getTree());
                    char_literal16=(Token)match(input,44,FOLLOW_44_in_source247);  
                    stream_44.add(char_literal16);



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
                    // 109:26: -> ^( MULTI multiSource )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:29: ^( MULTI multiSource )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:1: sourceEof : source EOF -> source ;
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
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:11: ( source EOF -> source )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:110:14: source EOF
            {
            pushFollow(FOLLOW_source_in_sourceEof265);
            source17=source();

            state._fsp--;

            stream_source.add(source17.getTree());
            EOF18=(Token)match(input,EOF,FOLLOW_EOF_in_sourceEof267);  
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
            // 110:27: -> source
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:1: singleSource : Identifier ( args )? -> ^( SOURCE Identifier ( args )? ) ;
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
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:14: ( Identifier ( args )? -> ^( SOURCE Identifier ( args )? ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:16: Identifier ( args )?
            {
            Identifier19=(Token)match(input,Identifier,FOLLOW_Identifier_in_singleSource280);  
            stream_Identifier.add(Identifier19);

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:27: ( args )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==57) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:27: args
                    {
                    pushFollow(FOLLOW_args_in_singleSource282);
                    args20=args();

                    state._fsp--;

                    stream_args.add(args20.getTree());

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
            // 111:33: -> ^( SOURCE Identifier ( args )? )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:36: ^( SOURCE Identifier ( args )? )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(SOURCE, "SOURCE"), root_1);

                adaptor.addChild(root_1, stream_Identifier.nextNode());
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:111:56: ( args )?
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:112:1: multiSource : singleSource ( ',' singleSource )* -> ( singleSource )+ ;
    public final FlumeDeployParser.multiSource_return multiSource() throws RecognitionException {
        FlumeDeployParser.multiSource_return retval = new FlumeDeployParser.multiSource_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal22=null;
        FlumeDeployParser.singleSource_return singleSource21 = null;

        FlumeDeployParser.singleSource_return singleSource23 = null;


        Object char_literal22_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleSubtreeStream stream_singleSource=new RewriteRuleSubtreeStream(adaptor,"rule singleSource");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:112:13: ( singleSource ( ',' singleSource )* -> ( singleSource )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:112:15: singleSource ( ',' singleSource )*
            {
            pushFollow(FOLLOW_singleSource_in_multiSource301);
            singleSource21=singleSource();

            state._fsp--;

            stream_singleSource.add(singleSource21.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:112:28: ( ',' singleSource )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0==45) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:112:29: ',' singleSource
            	    {
            	    char_literal22=(Token)match(input,45,FOLLOW_45_in_multiSource304);  
            	    stream_45.add(char_literal22);

            	    pushFollow(FOLLOW_singleSource_in_multiSource306);
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
            // 112:48: -> ( singleSource )+
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:115:1: sink : simpleSink -> simpleSink ;
    public final FlumeDeployParser.sink_return sink() throws RecognitionException {
        FlumeDeployParser.sink_return retval = new FlumeDeployParser.sink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        FlumeDeployParser.simpleSink_return simpleSink24 = null;


        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:115:7: ( simpleSink -> simpleSink )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:115:9: simpleSink
            {
            pushFollow(FOLLOW_simpleSink_in_sink325);
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
            // 115:20: -> simpleSink
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:117:1: singleSink : Identifier ( args )? -> ^( SINK Identifier ( args )? ) ;
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
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:117:12: ( Identifier ( args )? -> ^( SINK Identifier ( args )? ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:117:14: Identifier ( args )?
            {
            Identifier25=(Token)match(input,Identifier,FOLLOW_Identifier_in_singleSink337);  
            stream_Identifier.add(Identifier25);

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:117:25: ( args )?
            int alt5=2;
            alt5 = dfa5.predict(input);
            switch (alt5) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:117:25: args
                    {
                    pushFollow(FOLLOW_args_in_singleSink339);
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
            // 117:32: -> ^( SINK Identifier ( args )? )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:117:35: ^( SINK Identifier ( args )? )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(SINK, "SINK"), root_1);

                adaptor.addChild(root_1, stream_Identifier.nextNode());
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:117:53: ( args )?
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:119:1: sinkEof : simpleSink EOF ;
    public final FlumeDeployParser.sinkEof_return sinkEof() throws RecognitionException {
        FlumeDeployParser.sinkEof_return retval = new FlumeDeployParser.sinkEof_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token EOF28=null;
        FlumeDeployParser.simpleSink_return simpleSink27 = null;


        Object EOF28_tree=null;

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:119:10: ( simpleSink EOF )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:119:12: simpleSink EOF
            {
            root_0 = (Object)adaptor.nil();

            pushFollow(FOLLOW_simpleSink_in_sinkEof361);
            simpleSink27=simpleSink();

            state._fsp--;

            adaptor.addChild(root_0, simpleSink27.getTree());
            EOF28=(Token)match(input,EOF,FOLLOW_EOF_in_sinkEof363); 
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:121:1: simpleSink : ( '[' multiSink ']' -> ^( MULTI multiSink ) | '{' decoratedSink '}' -> ^( DECO decoratedSink ) | '<' failoverSink '>' -> ^( BACKUP failoverSink ) | letSink -> letSink | singleSink -> singleSink | rollSink -> rollSink | failoverChain -> failoverChain );
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
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_43=new RewriteRuleTokenStream(adaptor,"token 43");
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
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
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:121:12: ( '[' multiSink ']' -> ^( MULTI multiSink ) | '{' decoratedSink '}' -> ^( DECO decoratedSink ) | '<' failoverSink '>' -> ^( BACKUP failoverSink ) | letSink -> letSink | singleSink -> singleSink | rollSink -> rollSink | failoverChain -> failoverChain )
            int alt6=7;
            switch ( input.LA(1) ) {
            case 43:
                {
                alt6=1;
                }
                break;
            case 46:
                {
                alt6=2;
                }
                break;
            case 48:
                {
                alt6=3;
                }
                break;
            case 52:
                {
                alt6=4;
                }
                break;
            case Identifier:
                {
                alt6=5;
                }
                break;
            case 55:
                {
                alt6=6;
                }
                break;
            case 56:
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:121:14: '[' multiSink ']'
                    {
                    char_literal29=(Token)match(input,43,FOLLOW_43_in_simpleSink371);  
                    stream_43.add(char_literal29);

                    pushFollow(FOLLOW_multiSink_in_simpleSink373);
                    multiSink30=multiSink();

                    state._fsp--;

                    stream_multiSink.add(multiSink30.getTree());
                    char_literal31=(Token)match(input,44,FOLLOW_44_in_simpleSink375);  
                    stream_44.add(char_literal31);



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
                    // 121:34: -> ^( MULTI multiSink )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:121:37: ^( MULTI multiSink )
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:122:5: '{' decoratedSink '}'
                    {
                    char_literal32=(Token)match(input,46,FOLLOW_46_in_simpleSink392);  
                    stream_46.add(char_literal32);

                    pushFollow(FOLLOW_decoratedSink_in_simpleSink394);
                    decoratedSink33=decoratedSink();

                    state._fsp--;

                    stream_decoratedSink.add(decoratedSink33.getTree());
                    char_literal34=(Token)match(input,47,FOLLOW_47_in_simpleSink396);  
                    stream_47.add(char_literal34);



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
                    // 122:27: -> ^( DECO decoratedSink )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:122:30: ^( DECO decoratedSink )
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:123:5: '<' failoverSink '>'
                    {
                    char_literal35=(Token)match(input,48,FOLLOW_48_in_simpleSink410);  
                    stream_48.add(char_literal35);

                    pushFollow(FOLLOW_failoverSink_in_simpleSink412);
                    failoverSink36=failoverSink();

                    state._fsp--;

                    stream_failoverSink.add(failoverSink36.getTree());
                    char_literal37=(Token)match(input,49,FOLLOW_49_in_simpleSink414);  
                    stream_49.add(char_literal37);



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
                    // 123:26: -> ^( BACKUP failoverSink )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:123:29: ^( BACKUP failoverSink )
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:124:7: letSink
                    {
                    pushFollow(FOLLOW_letSink_in_simpleSink430);
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
                    // 124:31: -> letSink
                    {
                        adaptor.addChild(root_0, stream_letSink.nextTree());

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 5 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:125:5: singleSink
                    {
                    pushFollow(FOLLOW_singleSink_in_simpleSink456);
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
                    // 125:29: -> singleSink
                    {
                        adaptor.addChild(root_0, stream_singleSink.nextTree());

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 6 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:126:13: rollSink
                    {
                    pushFollow(FOLLOW_rollSink_in_simpleSink488);
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
                    // 126:37: -> rollSink
                    {
                        adaptor.addChild(root_0, stream_rollSink.nextTree());

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 7 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:127:13: failoverChain
                    {
                    pushFollow(FOLLOW_failoverChain_in_simpleSink521);
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
                    // 127:37: -> failoverChain
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:1: decoratedSink : singleSink '=>' sink -> singleSink sink ;
    public final FlumeDeployParser.decoratedSink_return decoratedSink() throws RecognitionException {
        FlumeDeployParser.decoratedSink_return retval = new FlumeDeployParser.decoratedSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token string_literal43=null;
        FlumeDeployParser.singleSink_return singleSink42 = null;

        FlumeDeployParser.sink_return sink44 = null;


        Object string_literal43_tree=null;
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_sink=new RewriteRuleSubtreeStream(adaptor,"rule sink");
        RewriteRuleSubtreeStream stream_singleSink=new RewriteRuleSubtreeStream(adaptor,"rule singleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:17: ( singleSink '=>' sink -> singleSink sink )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:20: singleSink '=>' sink
            {
            pushFollow(FOLLOW_singleSink_in_decoratedSink555);
            singleSink42=singleSink();

            state._fsp--;

            stream_singleSink.add(singleSink42.getTree());
            string_literal43=(Token)match(input,50,FOLLOW_50_in_decoratedSink557);  
            stream_50.add(string_literal43);

            pushFollow(FOLLOW_sink_in_decoratedSink559);
            sink44=sink();

            state._fsp--;

            stream_sink.add(sink44.getTree());


            // AST REWRITE
            // elements: singleSink, sink
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 130:44: -> singleSink sink
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:1: multiSink : simpleSink ( ',' simpleSink )* -> ( simpleSink )* ;
    public final FlumeDeployParser.multiSink_return multiSink() throws RecognitionException {
        FlumeDeployParser.multiSink_return retval = new FlumeDeployParser.multiSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal46=null;
        FlumeDeployParser.simpleSink_return simpleSink45 = null;

        FlumeDeployParser.simpleSink_return simpleSink47 = null;


        Object char_literal46_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:17: ( simpleSink ( ',' simpleSink )* -> ( simpleSink )* )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:20: simpleSink ( ',' simpleSink )*
            {
            pushFollow(FOLLOW_simpleSink_in_multiSink582);
            simpleSink45=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink45.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:31: ( ',' simpleSink )*
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( (LA7_0==45) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:32: ',' simpleSink
            	    {
            	    char_literal46=(Token)match(input,45,FOLLOW_45_in_multiSink585);  
            	    stream_45.add(char_literal46);

            	    pushFollow(FOLLOW_simpleSink_in_multiSink587);
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
            // 131:50: -> ( simpleSink )*
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:131:53: ( simpleSink )*
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:1: failoverSink : simpleSink ( '?' simpleSink )+ -> ( simpleSink )+ ;
    public final FlumeDeployParser.failoverSink_return failoverSink() throws RecognitionException {
        FlumeDeployParser.failoverSink_return retval = new FlumeDeployParser.failoverSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal49=null;
        FlumeDeployParser.simpleSink_return simpleSink48 = null;

        FlumeDeployParser.simpleSink_return simpleSink50 = null;


        Object char_literal49_tree=null;
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:17: ( simpleSink ( '?' simpleSink )+ -> ( simpleSink )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:20: simpleSink ( '?' simpleSink )+
            {
            pushFollow(FOLLOW_simpleSink_in_failoverSink607);
            simpleSink48=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink48.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:31: ( '?' simpleSink )+
            int cnt8=0;
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==51) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:32: '?' simpleSink
            	    {
            	    char_literal49=(Token)match(input,51,FOLLOW_51_in_failoverSink610);  
            	    stream_51.add(char_literal49);

            	    pushFollow(FOLLOW_simpleSink_in_failoverSink612);
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
            // 132:49: -> ( simpleSink )+
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:133:1: letSink : 'let' Identifier ':=' simpleSink 'in' simpleSink -> ^( LET Identifier ( simpleSink )+ ) ;
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
        RewriteRuleTokenStream stream_52=new RewriteRuleTokenStream(adaptor,"token 52");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_54=new RewriteRuleTokenStream(adaptor,"token 54");
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:133:17: ( 'let' Identifier ':=' simpleSink 'in' simpleSink -> ^( LET Identifier ( simpleSink )+ ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:133:20: 'let' Identifier ':=' simpleSink 'in' simpleSink
            {
            string_literal51=(Token)match(input,52,FOLLOW_52_in_letSink636);  
            stream_52.add(string_literal51);

            Identifier52=(Token)match(input,Identifier,FOLLOW_Identifier_in_letSink638);  
            stream_Identifier.add(Identifier52);

            string_literal53=(Token)match(input,53,FOLLOW_53_in_letSink640);  
            stream_53.add(string_literal53);

            pushFollow(FOLLOW_simpleSink_in_letSink642);
            simpleSink54=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink54.getTree());
            string_literal55=(Token)match(input,54,FOLLOW_54_in_letSink644);  
            stream_54.add(string_literal55);

            pushFollow(FOLLOW_simpleSink_in_letSink646);
            simpleSink56=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink56.getTree());


            // AST REWRITE
            // elements: simpleSink, Identifier
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 134:35: -> ^( LET Identifier ( simpleSink )+ )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:134:38: ^( LET Identifier ( simpleSink )+ )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:135:1: rollSink : 'roll' args '{' simpleSink '}' -> ^( ROLL simpleSink args ) ;
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
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:135:17: ( 'roll' args '{' simpleSink '}' -> ^( ROLL simpleSink args ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:135:20: 'roll' args '{' simpleSink '}'
            {
            string_literal57=(Token)match(input,55,FOLLOW_55_in_rollSink707);  
            stream_55.add(string_literal57);

            pushFollow(FOLLOW_args_in_rollSink709);
            args58=args();

            state._fsp--;

            stream_args.add(args58.getTree());
            char_literal59=(Token)match(input,46,FOLLOW_46_in_rollSink711);  
            stream_46.add(char_literal59);

            pushFollow(FOLLOW_simpleSink_in_rollSink713);
            simpleSink60=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink60.getTree());
            char_literal61=(Token)match(input,47,FOLLOW_47_in_rollSink715);  
            stream_47.add(char_literal61);



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
            // 136:35: -> ^( ROLL simpleSink args )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:136:38: ^( ROLL simpleSink args )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:137:1: failoverChain : 'failchain' args '{' simpleSink '}' -> ^( FAILCHAIN simpleSink args ) ;
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
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:137:17: ( 'failchain' args '{' simpleSink '}' -> ^( FAILCHAIN simpleSink args ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:137:20: 'failchain' args '{' simpleSink '}'
            {
            string_literal62=(Token)match(input,56,FOLLOW_56_in_failoverChain769);  
            stream_56.add(string_literal62);

            pushFollow(FOLLOW_args_in_failoverChain771);
            args63=args();

            state._fsp--;

            stream_args.add(args63.getTree());
            char_literal64=(Token)match(input,46,FOLLOW_46_in_failoverChain773);  
            stream_46.add(char_literal64);

            pushFollow(FOLLOW_simpleSink_in_failoverChain775);
            simpleSink65=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink65.getTree());
            char_literal66=(Token)match(input,47,FOLLOW_47_in_failoverChain777);  
            stream_47.add(char_literal66);



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
            // 138:35: -> ^( FAILCHAIN simpleSink args )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:138:38: ^( FAILCHAIN simpleSink args )
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

    public static class args_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "args"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:1: args : ( '(' ( arglist ( ',' kwarglist )? ) ')' -> arglist ( kwarglist )? | '(' kwarglist ')' -> ( kwarglist )? | '(' ')' ->);
    public final FlumeDeployParser.args_return args() throws RecognitionException {
        FlumeDeployParser.args_return retval = new FlumeDeployParser.args_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal67=null;
        Token char_literal69=null;
        Token char_literal71=null;
        Token char_literal72=null;
        Token char_literal74=null;
        Token char_literal75=null;
        Token char_literal76=null;
        FlumeDeployParser.arglist_return arglist68 = null;

        FlumeDeployParser.kwarglist_return kwarglist70 = null;

        FlumeDeployParser.kwarglist_return kwarglist73 = null;


        Object char_literal67_tree=null;
        Object char_literal69_tree=null;
        Object char_literal71_tree=null;
        Object char_literal72_tree=null;
        Object char_literal74_tree=null;
        Object char_literal75_tree=null;
        Object char_literal76_tree=null;
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleSubtreeStream stream_arglist=new RewriteRuleSubtreeStream(adaptor,"rule arglist");
        RewriteRuleSubtreeStream stream_kwarglist=new RewriteRuleSubtreeStream(adaptor,"rule kwarglist");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:9: ( '(' ( arglist ( ',' kwarglist )? ) ')' -> arglist ( kwarglist )? | '(' kwarglist ')' -> ( kwarglist )? | '(' ')' ->)
            int alt10=3;
            alt10 = dfa10.predict(input);
            switch (alt10) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:11: '(' ( arglist ( ',' kwarglist )? ) ')'
                    {
                    char_literal67=(Token)match(input,57,FOLLOW_57_in_args832);  
                    stream_57.add(char_literal67);

                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:15: ( arglist ( ',' kwarglist )? )
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:17: arglist ( ',' kwarglist )?
                    {
                    pushFollow(FOLLOW_arglist_in_args836);
                    arglist68=arglist();

                    state._fsp--;

                    stream_arglist.add(arglist68.getTree());
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:25: ( ',' kwarglist )?
                    int alt9=2;
                    int LA9_0 = input.LA(1);

                    if ( (LA9_0==45) ) {
                        alt9=1;
                    }
                    switch (alt9) {
                        case 1 :
                            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:26: ',' kwarglist
                            {
                            char_literal69=(Token)match(input,45,FOLLOW_45_in_args839);  
                            stream_45.add(char_literal69);

                            pushFollow(FOLLOW_kwarglist_in_args841);
                            kwarglist70=kwarglist();

                            state._fsp--;

                            stream_kwarglist.add(kwarglist70.getTree());

                            }
                            break;

                    }


                    }

                    char_literal71=(Token)match(input,58,FOLLOW_58_in_args848);  
                    stream_58.add(char_literal71);



                    // AST REWRITE
                    // elements: arglist, kwarglist
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 140:49: -> arglist ( kwarglist )?
                    {
                        adaptor.addChild(root_0, stream_arglist.nextTree());
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:140:60: ( kwarglist )?
                        if ( stream_kwarglist.hasNext() ) {
                            adaptor.addChild(root_0, stream_kwarglist.nextTree());

                        }
                        stream_kwarglist.reset();

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:141:11: '(' kwarglist ')'
                    {
                    char_literal72=(Token)match(input,57,FOLLOW_57_in_args867);  
                    stream_57.add(char_literal72);

                    pushFollow(FOLLOW_kwarglist_in_args869);
                    kwarglist73=kwarglist();

                    state._fsp--;

                    stream_kwarglist.add(kwarglist73.getTree());
                    char_literal74=(Token)match(input,58,FOLLOW_58_in_args871);  
                    stream_58.add(char_literal74);



                    // AST REWRITE
                    // elements: kwarglist
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 141:29: -> ( kwarglist )?
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:141:32: ( kwarglist )?
                        if ( stream_kwarglist.hasNext() ) {
                            adaptor.addChild(root_0, stream_kwarglist.nextTree());

                        }
                        stream_kwarglist.reset();

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:142:11: '(' ')'
                    {
                    char_literal75=(Token)match(input,57,FOLLOW_57_in_args889);  
                    stream_57.add(char_literal75);

                    char_literal76=(Token)match(input,58,FOLLOW_58_in_args891);  
                    stream_58.add(char_literal76);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 142:19: ->
                    {
                        root_0 = null;
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
    // $ANTLR end "args"

    public static class arglist_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arglist"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:145:1: arglist : literal ( ',' literal )* -> ( literal )+ ;
    public final FlumeDeployParser.arglist_return arglist() throws RecognitionException {
        FlumeDeployParser.arglist_return retval = new FlumeDeployParser.arglist_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal78=null;
        FlumeDeployParser.literal_return literal77 = null;

        FlumeDeployParser.literal_return literal79 = null;


        Object char_literal78_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleSubtreeStream stream_literal=new RewriteRuleSubtreeStream(adaptor,"rule literal");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:145:9: ( literal ( ',' literal )* -> ( literal )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:145:11: literal ( ',' literal )*
            {
            pushFollow(FOLLOW_literal_in_arglist911);
            literal77=literal();

            state._fsp--;

            stream_literal.add(literal77.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:145:19: ( ',' literal )*
            loop11:
            do {
                int alt11=2;
                alt11 = dfa11.predict(input);
                switch (alt11) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:145:20: ',' literal
            	    {
            	    char_literal78=(Token)match(input,45,FOLLOW_45_in_arglist914);  
            	    stream_45.add(char_literal78);

            	    pushFollow(FOLLOW_literal_in_arglist916);
            	    literal79=literal();

            	    state._fsp--;

            	    stream_literal.add(literal79.getTree());

            	    }
            	    break;

            	default :
            	    break loop11;
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
            // 145:34: -> ( literal )+
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

    public static class kwarglist_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "kwarglist"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:147:1: kwarglist : kwarg ( ',' kwarg )* -> ( kwarg )+ ;
    public final FlumeDeployParser.kwarglist_return kwarglist() throws RecognitionException {
        FlumeDeployParser.kwarglist_return retval = new FlumeDeployParser.kwarglist_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal81=null;
        FlumeDeployParser.kwarg_return kwarg80 = null;

        FlumeDeployParser.kwarg_return kwarg82 = null;


        Object char_literal81_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleSubtreeStream stream_kwarg=new RewriteRuleSubtreeStream(adaptor,"rule kwarg");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:147:11: ( kwarg ( ',' kwarg )* -> ( kwarg )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:147:13: kwarg ( ',' kwarg )*
            {
            pushFollow(FOLLOW_kwarg_in_kwarglist932);
            kwarg80=kwarg();

            state._fsp--;

            stream_kwarg.add(kwarg80.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:147:19: ( ',' kwarg )*
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( (LA12_0==45) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:147:20: ',' kwarg
            	    {
            	    char_literal81=(Token)match(input,45,FOLLOW_45_in_kwarglist935);  
            	    stream_45.add(char_literal81);

            	    pushFollow(FOLLOW_kwarg_in_kwarglist937);
            	    kwarg82=kwarg();

            	    state._fsp--;

            	    stream_kwarg.add(kwarg82.getTree());

            	    }
            	    break;

            	default :
            	    break loop12;
                }
            } while (true);



            // AST REWRITE
            // elements: kwarg
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 147:32: -> ( kwarg )+
            {
                if ( !(stream_kwarg.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_kwarg.hasNext() ) {
                    adaptor.addChild(root_0, stream_kwarg.nextTree());

                }
                stream_kwarg.reset();

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
    // $ANTLR end "kwarglist"

    public static class kwarg_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "kwarg"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:149:1: kwarg : Identifier '=' literal -> ^( KWARG Identifier literal ) ;
    public final FlumeDeployParser.kwarg_return kwarg() throws RecognitionException {
        FlumeDeployParser.kwarg_return retval = new FlumeDeployParser.kwarg_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token Identifier83=null;
        Token char_literal84=null;
        FlumeDeployParser.literal_return literal85 = null;


        Object Identifier83_tree=null;
        Object char_literal84_tree=null;
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleSubtreeStream stream_literal=new RewriteRuleSubtreeStream(adaptor,"rule literal");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:149:9: ( Identifier '=' literal -> ^( KWARG Identifier literal ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:149:13: Identifier '=' literal
            {
            Identifier83=(Token)match(input,Identifier,FOLLOW_Identifier_in_kwarg956);  
            stream_Identifier.add(Identifier83);

            char_literal84=(Token)match(input,59,FOLLOW_59_in_kwarg958);  
            stream_59.add(char_literal84);

            pushFollow(FOLLOW_literal_in_kwarg960);
            literal85=literal();

            state._fsp--;

            stream_literal.add(literal85.getTree());


            // AST REWRITE
            // elements: Identifier, literal
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 149:36: -> ^( KWARG Identifier literal )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:149:39: ^( KWARG Identifier literal )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(KWARG, "KWARG"), root_1);

                adaptor.addChild(root_1, stream_Identifier.nextNode());
                adaptor.addChild(root_1, stream_literal.nextTree());

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
    // $ANTLR end "kwarg"

    public static class literal_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "literal"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:152:1: literal : ( integerLiteral | StringLiteral -> ^( STRING StringLiteral ) | booleanLiteral | FloatingPointLiteral -> ^( FLOAT FloatingPointLiteral ) );
    public final FlumeDeployParser.literal_return literal() throws RecognitionException {
        FlumeDeployParser.literal_return retval = new FlumeDeployParser.literal_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token StringLiteral87=null;
        Token FloatingPointLiteral89=null;
        FlumeDeployParser.integerLiteral_return integerLiteral86 = null;

        FlumeDeployParser.booleanLiteral_return booleanLiteral88 = null;


        Object StringLiteral87_tree=null;
        Object FloatingPointLiteral89_tree=null;
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleTokenStream stream_FloatingPointLiteral=new RewriteRuleTokenStream(adaptor,"token FloatingPointLiteral");

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:153:5: ( integerLiteral | StringLiteral -> ^( STRING StringLiteral ) | booleanLiteral | FloatingPointLiteral -> ^( FLOAT FloatingPointLiteral ) )
            int alt13=4;
            switch ( input.LA(1) ) {
            case HexLiteral:
            case OctalLiteral:
            case DecimalLiteral:
                {
                alt13=1;
                }
                break;
            case StringLiteral:
                {
                alt13=2;
                }
                break;
            case 60:
            case 61:
                {
                alt13=3;
                }
                break;
            case FloatingPointLiteral:
                {
                alt13=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 13, 0, input);

                throw nvae;
            }

            switch (alt13) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:153:9: integerLiteral
                    {
                    root_0 = (Object)adaptor.nil();

                    pushFollow(FOLLOW_integerLiteral_in_literal987);
                    integerLiteral86=integerLiteral();

                    state._fsp--;

                    adaptor.addChild(root_0, integerLiteral86.getTree());

                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:154:9: StringLiteral
                    {
                    StringLiteral87=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_literal997);  
                    stream_StringLiteral.add(StringLiteral87);



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
                    // 154:24: -> ^( STRING StringLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:154:27: ^( STRING StringLiteral )
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:155:9: booleanLiteral
                    {
                    root_0 = (Object)adaptor.nil();

                    pushFollow(FOLLOW_booleanLiteral_in_literal1016);
                    booleanLiteral88=booleanLiteral();

                    state._fsp--;

                    adaptor.addChild(root_0, booleanLiteral88.getTree());

                    }
                    break;
                case 4 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:156:9: FloatingPointLiteral
                    {
                    FloatingPointLiteral89=(Token)match(input,FloatingPointLiteral,FOLLOW_FloatingPointLiteral_in_literal1026);  
                    stream_FloatingPointLiteral.add(FloatingPointLiteral89);



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
                    // 156:30: -> ^( FLOAT FloatingPointLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:156:33: ^( FLOAT FloatingPointLiteral )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:158:1: integerLiteral : ( HexLiteral -> ^( HEX HexLiteral ) | OctalLiteral -> ^( OCT OctalLiteral ) | DecimalLiteral -> ^( DEC DecimalLiteral ) );
    public final FlumeDeployParser.integerLiteral_return integerLiteral() throws RecognitionException {
        FlumeDeployParser.integerLiteral_return retval = new FlumeDeployParser.integerLiteral_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token HexLiteral90=null;
        Token OctalLiteral91=null;
        Token DecimalLiteral92=null;

        Object HexLiteral90_tree=null;
        Object OctalLiteral91_tree=null;
        Object DecimalLiteral92_tree=null;
        RewriteRuleTokenStream stream_HexLiteral=new RewriteRuleTokenStream(adaptor,"token HexLiteral");
        RewriteRuleTokenStream stream_DecimalLiteral=new RewriteRuleTokenStream(adaptor,"token DecimalLiteral");
        RewriteRuleTokenStream stream_OctalLiteral=new RewriteRuleTokenStream(adaptor,"token OctalLiteral");

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:159:5: ( HexLiteral -> ^( HEX HexLiteral ) | OctalLiteral -> ^( OCT OctalLiteral ) | DecimalLiteral -> ^( DEC DecimalLiteral ) )
            int alt14=3;
            switch ( input.LA(1) ) {
            case HexLiteral:
                {
                alt14=1;
                }
                break;
            case OctalLiteral:
                {
                alt14=2;
                }
                break;
            case DecimalLiteral:
                {
                alt14=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;
            }

            switch (alt14) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:159:9: HexLiteral
                    {
                    HexLiteral90=(Token)match(input,HexLiteral,FOLLOW_HexLiteral_in_integerLiteral1052);  
                    stream_HexLiteral.add(HexLiteral90);



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
                    // 159:21: -> ^( HEX HexLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:159:24: ^( HEX HexLiteral )
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:160:9: OctalLiteral
                    {
                    OctalLiteral91=(Token)match(input,OctalLiteral,FOLLOW_OctalLiteral_in_integerLiteral1071);  
                    stream_OctalLiteral.add(OctalLiteral91);



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
                    // 160:23: -> ^( OCT OctalLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:160:26: ^( OCT OctalLiteral )
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:161:9: DecimalLiteral
                    {
                    DecimalLiteral92=(Token)match(input,DecimalLiteral,FOLLOW_DecimalLiteral_in_integerLiteral1090);  
                    stream_DecimalLiteral.add(DecimalLiteral92);



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
                    // 161:25: -> ^( DEC DecimalLiteral )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:161:28: ^( DEC DecimalLiteral )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:164:1: booleanLiteral : ( 'true' -> ^( BOOL 'true' ) | 'false' -> ^( BOOL 'false' ) );
    public final FlumeDeployParser.booleanLiteral_return booleanLiteral() throws RecognitionException {
        FlumeDeployParser.booleanLiteral_return retval = new FlumeDeployParser.booleanLiteral_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token string_literal93=null;
        Token string_literal94=null;

        Object string_literal93_tree=null;
        Object string_literal94_tree=null;
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleTokenStream stream_61=new RewriteRuleTokenStream(adaptor,"token 61");

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:165:5: ( 'true' -> ^( BOOL 'true' ) | 'false' -> ^( BOOL 'false' ) )
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( (LA15_0==60) ) {
                alt15=1;
            }
            else if ( (LA15_0==61) ) {
                alt15=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 15, 0, input);

                throw nvae;
            }
            switch (alt15) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:165:9: 'true'
                    {
                    string_literal93=(Token)match(input,60,FOLLOW_60_in_booleanLiteral1122);  
                    stream_60.add(string_literal93);



                    // AST REWRITE
                    // elements: 60
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 165:18: -> ^( BOOL 'true' )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:165:21: ^( BOOL 'true' )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(BOOL, "BOOL"), root_1);

                        adaptor.addChild(root_1, stream_60.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:166:9: 'false'
                    {
                    string_literal94=(Token)match(input,61,FOLLOW_61_in_booleanLiteral1142);  
                    stream_61.add(string_literal94);



                    // AST REWRITE
                    // elements: 61
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 166:18: -> ^( BOOL 'false' )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:166:21: ^( BOOL 'false' )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(BOOL, "BOOL"), root_1);

                        adaptor.addChild(root_1, stream_61.nextNode());

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
    protected DFA10 dfa10 = new DFA10(this);
    protected DFA11 dfa11 = new DFA11(this);
    static final String DFA5_eotS =
        "\13\uffff";
    static final String DFA5_eofS =
        "\1\2\12\uffff";
    static final String DFA5_minS =
        "\1\52\12\uffff";
    static final String DFA5_maxS =
        "\1\71\12\uffff";
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
            return "117:25: ( args )?";
        }
    }
    static final String DFA10_eotS =
        "\13\uffff";
    static final String DFA10_eofS =
        "\13\uffff";
    static final String DFA10_minS =
        "\1\71\1\25\11\uffff";
    static final String DFA10_maxS =
        "\1\71\1\75\11\uffff";
    static final String DFA10_acceptS =
        "\2\uffff\1\3\1\1\6\uffff\1\2";
    static final String DFA10_specialS =
        "\13\uffff}>";
    static final String[] DFA10_transitionS = {
            "\1\1",
            "\1\12\1\uffff\5\3\36\uffff\1\2\1\uffff\2\3",
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

    static final short[] DFA10_eot = DFA.unpackEncodedString(DFA10_eotS);
    static final short[] DFA10_eof = DFA.unpackEncodedString(DFA10_eofS);
    static final char[] DFA10_min = DFA.unpackEncodedStringToUnsignedChars(DFA10_minS);
    static final char[] DFA10_max = DFA.unpackEncodedStringToUnsignedChars(DFA10_maxS);
    static final short[] DFA10_accept = DFA.unpackEncodedString(DFA10_acceptS);
    static final short[] DFA10_special = DFA.unpackEncodedString(DFA10_specialS);
    static final short[][] DFA10_transition;

    static {
        int numStates = DFA10_transitionS.length;
        DFA10_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA10_transition[i] = DFA.unpackEncodedString(DFA10_transitionS[i]);
        }
    }

    class DFA10 extends DFA {

        public DFA10(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 10;
            this.eot = DFA10_eot;
            this.eof = DFA10_eof;
            this.min = DFA10_min;
            this.max = DFA10_max;
            this.accept = DFA10_accept;
            this.special = DFA10_special;
            this.transition = DFA10_transition;
        }
        public String getDescription() {
            return "140:1: args : ( '(' ( arglist ( ',' kwarglist )? ) ')' -> arglist ( kwarglist )? | '(' kwarglist ')' -> ( kwarglist )? | '(' ')' ->);";
        }
    }
    static final String DFA11_eotS =
        "\13\uffff";
    static final String DFA11_eofS =
        "\13\uffff";
    static final String DFA11_minS =
        "\1\55\1\25\11\uffff";
    static final String DFA11_maxS =
        "\1\72\1\75\11\uffff";
    static final String DFA11_acceptS =
        "\2\uffff\1\2\1\uffff\1\1\6\uffff";
    static final String DFA11_specialS =
        "\13\uffff}>";
    static final String[] DFA11_transitionS = {
            "\1\1\14\uffff\1\2",
            "\1\2\1\uffff\5\4\40\uffff\2\4",
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

    static final short[] DFA11_eot = DFA.unpackEncodedString(DFA11_eotS);
    static final short[] DFA11_eof = DFA.unpackEncodedString(DFA11_eofS);
    static final char[] DFA11_min = DFA.unpackEncodedStringToUnsignedChars(DFA11_minS);
    static final char[] DFA11_max = DFA.unpackEncodedStringToUnsignedChars(DFA11_maxS);
    static final short[] DFA11_accept = DFA.unpackEncodedString(DFA11_acceptS);
    static final short[] DFA11_special = DFA.unpackEncodedString(DFA11_specialS);
    static final short[][] DFA11_transition;

    static {
        int numStates = DFA11_transitionS.length;
        DFA11_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA11_transition[i] = DFA.unpackEncodedString(DFA11_transitionS[i]);
        }
    }

    class DFA11 extends DFA {

        public DFA11(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 11;
            this.eot = DFA11_eot;
            this.eof = DFA11_eof;
            this.min = DFA11_min;
            this.max = DFA11_max;
            this.accept = DFA11_accept;
            this.special = DFA11_special;
            this.transition = DFA11_transition;
        }
        public String getDescription() {
            return "()* loopback of 145:19: ( ',' literal )*";
        }
    }
 

    public static final BitSet FOLLOW_def_in_deflist145 = new BitSet(new long[]{0x0000000000600000L});
    public static final BitSet FOLLOW_EOF_in_deflist148 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_host_in_def158 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_40_in_def160 = new BitSet(new long[]{0x0000080000200000L});
    public static final BitSet FOLLOW_source_in_def162 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_41_in_def164 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_sink_in_def166 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_def169 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_host0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_source_in_connection205 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_41_in_connection207 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_sink_in_connection209 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSource_in_source231 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_43_in_source243 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_multiSource_in_source245 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_44_in_source247 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_source_in_sourceEof265 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_sourceEof267 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_singleSource280 = new BitSet(new long[]{0x0200000000000002L});
    public static final BitSet FOLLOW_args_in_singleSource282 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSource_in_multiSource301 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_45_in_multiSource304 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_singleSource_in_multiSource306 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_simpleSink_in_sink325 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_singleSink337 = new BitSet(new long[]{0x0200000000000002L});
    public static final BitSet FOLLOW_args_in_singleSink339 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_simpleSink_in_sinkEof361 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_sinkEof363 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_43_in_simpleSink371 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_multiSink_in_simpleSink373 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_44_in_simpleSink375 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_46_in_simpleSink392 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_decoratedSink_in_simpleSink394 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_simpleSink396 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_48_in_simpleSink410 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_failoverSink_in_simpleSink412 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_simpleSink414 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_letSink_in_simpleSink430 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSink_in_simpleSink456 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rollSink_in_simpleSink488 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_failoverChain_in_simpleSink521 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSink_in_decoratedSink555 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_decoratedSink557 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_sink_in_decoratedSink559 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_simpleSink_in_multiSink582 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_45_in_multiSink585 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_simpleSink_in_multiSink587 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_simpleSink_in_failoverSink607 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_51_in_failoverSink610 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_simpleSink_in_failoverSink612 = new BitSet(new long[]{0x0008000000000002L});
    public static final BitSet FOLLOW_52_in_letSink636 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_Identifier_in_letSink638 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_letSink640 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_simpleSink_in_letSink642 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_54_in_letSink644 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_simpleSink_in_letSink646 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_rollSink707 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_args_in_rollSink709 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_rollSink711 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_simpleSink_in_rollSink713 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_rollSink715 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_56_in_failoverChain769 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_args_in_failoverChain771 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_failoverChain773 = new BitSet(new long[]{0x0191480000200000L});
    public static final BitSet FOLLOW_simpleSink_in_failoverChain775 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_failoverChain777 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_args832 = new BitSet(new long[]{0x300000000F800000L});
    public static final BitSet FOLLOW_arglist_in_args836 = new BitSet(new long[]{0x0400200000000000L});
    public static final BitSet FOLLOW_45_in_args839 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_kwarglist_in_args841 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_58_in_args848 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_args867 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_kwarglist_in_args869 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_58_in_args871 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_args889 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_58_in_args891 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_arglist911 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_45_in_arglist914 = new BitSet(new long[]{0x300000000F800000L});
    public static final BitSet FOLLOW_literal_in_arglist916 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_kwarg_in_kwarglist932 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_45_in_kwarglist935 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_kwarg_in_kwarglist937 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_Identifier_in_kwarg956 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_59_in_kwarg958 = new BitSet(new long[]{0x300000000F800000L});
    public static final BitSet FOLLOW_literal_in_kwarg960 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_integerLiteral_in_literal987 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_literal997 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_booleanLiteral_in_literal1016 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FloatingPointLiteral_in_literal1026 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HexLiteral_in_integerLiteral1052 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_OctalLiteral_in_integerLiteral1071 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DecimalLiteral_in_integerLiteral1090 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_60_in_booleanLiteral1122 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_61_in_booleanLiteral1142 = new BitSet(new long[]{0x0000000000000002L});

}