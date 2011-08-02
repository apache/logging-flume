// $ANTLR 3.1.3 Mar 18, 2009 10:09:25 /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g 2011-01-01 12:44:10

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
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "NODE", "BLANK", "SINK", "BACKUP", "ROLL", "DECO", "SOURCE", "MULTI", "HEX", "OCT", "DEC", "STRING", "BOOL", "FLOAT", "KWARG", "Identifier", "IPLiteral", "StringLiteral", "FloatingPointLiteral", "HexLiteral", "OctalLiteral", "DecimalLiteral", "HexDigit", "IntegerTypeSuffix", "EscapeSequence", "UnicodeEscape", "OctalEscape", "Letter", "JavaIDDigit", "Exponent", "FloatTypeSuffix", "WS", "COMMENT", "LINE_COMMENT", "':'", "'|'", "';'", "','", "'['", "']'", "'{'", "'}'", "'<'", "'>'", "'=>'", "'?'", "'roll'", "'('", "')'", "'='", "'true'", "'false'"
    };
    public static final int DEC=14;
    public static final int FloatTypeSuffix=34;
    public static final int OctalLiteral=24;
    public static final int Exponent=33;
    public static final int SOURCE=10;
    public static final int FLOAT=17;
    public static final int MULTI=11;
    public static final int EOF=-1;
    public static final int HexDigit=26;
    public static final int SINK=6;
    public static final int Identifier=19;
    public static final int T__55=55;
    public static final int T__51=51;
    public static final int T__52=52;
    public static final int T__53=53;
    public static final int T__54=54;
    public static final int HEX=12;
    public static final int IPLiteral=20;
    public static final int COMMENT=36;
    public static final int T__50=50;
    public static final int T__42=42;
    public static final int T__43=43;
    public static final int HexLiteral=23;
    public static final int T__40=40;
    public static final int T__41=41;
    public static final int T__46=46;
    public static final int T__47=47;
    public static final int T__44=44;
    public static final int NODE=4;
    public static final int T__45=45;
    public static final int LINE_COMMENT=37;
    public static final int IntegerTypeSuffix=27;
    public static final int T__48=48;
    public static final int T__49=49;
    public static final int ROLL=8;
    public static final int BLANK=5;
    public static final int BOOL=16;
    public static final int KWARG=18;
    public static final int DecimalLiteral=25;
    public static final int BACKUP=7;
    public static final int OCT=13;
    public static final int StringLiteral=21;
    public static final int WS=35;
    public static final int T__38=38;
    public static final int T__39=39;
    public static final int UnicodeEscape=29;
    public static final int DECO=9;
    public static final int FloatingPointLiteral=22;
    public static final int JavaIDDigit=32;
    public static final int Letter=31;
    public static final int OctalEscape=30;
    public static final int EscapeSequence=28;
    public static final int STRING=15;

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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:95:1: deflist : ( def )* EOF ;
    public final FlumeDeployParser.deflist_return deflist() throws RecognitionException {
        FlumeDeployParser.deflist_return retval = new FlumeDeployParser.deflist_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token EOF2=null;
        FlumeDeployParser.def_return def1 = null;


        Object EOF2_tree=null;

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:95:9: ( ( def )* EOF )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:95:11: ( def )* EOF
            {
            root_0 = (Object)adaptor.nil();

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:95:11: ( def )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>=Identifier && LA1_0<=IPLiteral)) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:95:11: def
            	    {
            	    pushFollow(FOLLOW_def_in_deflist135);
            	    def1=def();

            	    state._fsp--;

            	    adaptor.addChild(root_0, def1.getTree());

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            EOF2=(Token)match(input,EOF,FOLLOW_EOF_in_deflist138); 
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:97:1: def : host ':' source '|' sink ';' -> ^( NODE host source sink ) ;
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
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleTokenStream stream_38=new RewriteRuleTokenStream(adaptor,"token 38");
        RewriteRuleSubtreeStream stream_host=new RewriteRuleSubtreeStream(adaptor,"rule host");
        RewriteRuleSubtreeStream stream_source=new RewriteRuleSubtreeStream(adaptor,"rule source");
        RewriteRuleSubtreeStream stream_sink=new RewriteRuleSubtreeStream(adaptor,"rule sink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:97:5: ( host ':' source '|' sink ';' -> ^( NODE host source sink ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:97:7: host ':' source '|' sink ';'
            {
            pushFollow(FOLLOW_host_in_def148);
            host3=host();

            state._fsp--;

            stream_host.add(host3.getTree());
            char_literal4=(Token)match(input,38,FOLLOW_38_in_def150);  
            stream_38.add(char_literal4);

            pushFollow(FOLLOW_source_in_def152);
            source5=source();

            state._fsp--;

            stream_source.add(source5.getTree());
            char_literal6=(Token)match(input,39,FOLLOW_39_in_def154);  
            stream_39.add(char_literal6);

            pushFollow(FOLLOW_sink_in_def156);
            sink7=sink();

            state._fsp--;

            stream_sink.add(sink7.getTree());
            char_literal8=(Token)match(input,40,FOLLOW_40_in_def159);  
            stream_40.add(char_literal8);



            // AST REWRITE
            // elements: source, sink, host
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (Object)adaptor.nil();
            // 97:37: -> ^( NODE host source sink )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:97:40: ^( NODE host source sink )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:99:1: host : ( Identifier | IPLiteral );
    public final FlumeDeployParser.host_return host() throws RecognitionException {
        FlumeDeployParser.host_return retval = new FlumeDeployParser.host_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token set9=null;

        Object set9_tree=null;

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:99:5: ( Identifier | IPLiteral )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:103:1: connection : source '|' sink -> ^( NODE BLANK source sink ) ;
    public final FlumeDeployParser.connection_return connection() throws RecognitionException {
        FlumeDeployParser.connection_return retval = new FlumeDeployParser.connection_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal11=null;
        FlumeDeployParser.source_return source10 = null;

        FlumeDeployParser.sink_return sink12 = null;


        Object char_literal11_tree=null;
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleSubtreeStream stream_source=new RewriteRuleSubtreeStream(adaptor,"rule source");
        RewriteRuleSubtreeStream stream_sink=new RewriteRuleSubtreeStream(adaptor,"rule sink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:104:2: ( source '|' sink -> ^( NODE BLANK source sink ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:104:5: source '|' sink
            {
            pushFollow(FOLLOW_source_in_connection195);
            source10=source();

            state._fsp--;

            stream_source.add(source10.getTree());
            char_literal11=(Token)match(input,39,FOLLOW_39_in_connection197);  
            stream_39.add(char_literal11);

            pushFollow(FOLLOW_sink_in_connection199);
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
            // 104:21: -> ^( NODE BLANK source sink )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:104:24: ^( NODE BLANK source sink )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:106:1: source : singleSource -> singleSource ;
    public final FlumeDeployParser.source_return source() throws RecognitionException {
        FlumeDeployParser.source_return retval = new FlumeDeployParser.source_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        FlumeDeployParser.singleSource_return singleSource13 = null;


        RewriteRuleSubtreeStream stream_singleSource=new RewriteRuleSubtreeStream(adaptor,"rule singleSource");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:106:10: ( singleSource -> singleSource )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:106:12: singleSource
            {
            pushFollow(FOLLOW_singleSource_in_source221);
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
            // 106:26: -> singleSource
            {
                adaptor.addChild(root_0, stream_singleSource.nextTree());

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
    // $ANTLR end "source"

    public static class sourceEof_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "sourceEof"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:107:1: sourceEof : source EOF -> source ;
    public final FlumeDeployParser.sourceEof_return sourceEof() throws RecognitionException {
        FlumeDeployParser.sourceEof_return retval = new FlumeDeployParser.sourceEof_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token EOF15=null;
        FlumeDeployParser.source_return source14 = null;


        Object EOF15_tree=null;
        RewriteRuleTokenStream stream_EOF=new RewriteRuleTokenStream(adaptor,"token EOF");
        RewriteRuleSubtreeStream stream_source=new RewriteRuleSubtreeStream(adaptor,"rule source");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:107:11: ( source EOF -> source )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:107:14: source EOF
            {
            pushFollow(FOLLOW_source_in_sourceEof235);
            source14=source();

            state._fsp--;

            stream_source.add(source14.getTree());
            EOF15=(Token)match(input,EOF,FOLLOW_EOF_in_sourceEof237);  
            stream_EOF.add(EOF15);



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
            // 107:27: -> source
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:1: singleSource : Identifier ( args )? -> ^( SOURCE Identifier ( args )? ) ;
    public final FlumeDeployParser.singleSource_return singleSource() throws RecognitionException {
        FlumeDeployParser.singleSource_return retval = new FlumeDeployParser.singleSource_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token Identifier16=null;
        FlumeDeployParser.args_return args17 = null;


        Object Identifier16_tree=null;
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:14: ( Identifier ( args )? -> ^( SOURCE Identifier ( args )? ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:16: Identifier ( args )?
            {
            Identifier16=(Token)match(input,Identifier,FOLLOW_Identifier_in_singleSource250);  
            stream_Identifier.add(Identifier16);

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:27: ( args )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==51) ) {
                alt2=1;
            }
            switch (alt2) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:27: args
                    {
                    pushFollow(FOLLOW_args_in_singleSource252);
                    args17=args();

                    state._fsp--;

                    stream_args.add(args17.getTree());

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
            // 108:33: -> ^( SOURCE Identifier ( args )? )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:36: ^( SOURCE Identifier ( args )? )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(SOURCE, "SOURCE"), root_1);

                adaptor.addChild(root_1, stream_Identifier.nextNode());
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:108:56: ( args )?
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:1: multiSource : singleSource ( ',' singleSource )* -> ( singleSource )+ ;
    public final FlumeDeployParser.multiSource_return multiSource() throws RecognitionException {
        FlumeDeployParser.multiSource_return retval = new FlumeDeployParser.multiSource_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal19=null;
        FlumeDeployParser.singleSource_return singleSource18 = null;

        FlumeDeployParser.singleSource_return singleSource20 = null;


        Object char_literal19_tree=null;
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_singleSource=new RewriteRuleSubtreeStream(adaptor,"rule singleSource");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:13: ( singleSource ( ',' singleSource )* -> ( singleSource )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:15: singleSource ( ',' singleSource )*
            {
            pushFollow(FOLLOW_singleSource_in_multiSource271);
            singleSource18=singleSource();

            state._fsp--;

            stream_singleSource.add(singleSource18.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:28: ( ',' singleSource )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==41) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:109:29: ',' singleSource
            	    {
            	    char_literal19=(Token)match(input,41,FOLLOW_41_in_multiSource274);  
            	    stream_41.add(char_literal19);

            	    pushFollow(FOLLOW_singleSource_in_multiSource276);
            	    singleSource20=singleSource();

            	    state._fsp--;

            	    stream_singleSource.add(singleSource20.getTree());

            	    }
            	    break;

            	default :
            	    break loop3;
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
            // 109:48: -> ( singleSource )+
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:112:1: sink : simpleSink -> simpleSink ;
    public final FlumeDeployParser.sink_return sink() throws RecognitionException {
        FlumeDeployParser.sink_return retval = new FlumeDeployParser.sink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        FlumeDeployParser.simpleSink_return simpleSink21 = null;


        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:112:7: ( simpleSink -> simpleSink )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:112:9: simpleSink
            {
            pushFollow(FOLLOW_simpleSink_in_sink295);
            simpleSink21=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink21.getTree());


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
            // 112:20: -> simpleSink
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:1: singleSink : Identifier ( args )? -> ^( SINK Identifier ( args )? ) ;
    public final FlumeDeployParser.singleSink_return singleSink() throws RecognitionException {
        FlumeDeployParser.singleSink_return retval = new FlumeDeployParser.singleSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token Identifier22=null;
        FlumeDeployParser.args_return args23 = null;


        Object Identifier22_tree=null;
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:12: ( Identifier ( args )? -> ^( SINK Identifier ( args )? ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:14: Identifier ( args )?
            {
            Identifier22=(Token)match(input,Identifier,FOLLOW_Identifier_in_singleSink307);  
            stream_Identifier.add(Identifier22);

            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:25: ( args )?
            int alt4=2;
            alt4 = dfa4.predict(input);
            switch (alt4) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:25: args
                    {
                    pushFollow(FOLLOW_args_in_singleSink309);
                    args23=args();

                    state._fsp--;

                    stream_args.add(args23.getTree());

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
            // 114:32: -> ^( SINK Identifier ( args )? )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:35: ^( SINK Identifier ( args )? )
                {
                Object root_1 = (Object)adaptor.nil();
                root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(SINK, "SINK"), root_1);

                adaptor.addChild(root_1, stream_Identifier.nextNode());
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:114:53: ( args )?
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:1: sinkEof : simpleSink EOF ;
    public final FlumeDeployParser.sinkEof_return sinkEof() throws RecognitionException {
        FlumeDeployParser.sinkEof_return retval = new FlumeDeployParser.sinkEof_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token EOF25=null;
        FlumeDeployParser.simpleSink_return simpleSink24 = null;


        Object EOF25_tree=null;

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:10: ( simpleSink EOF )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:116:12: simpleSink EOF
            {
            root_0 = (Object)adaptor.nil();

            pushFollow(FOLLOW_simpleSink_in_sinkEof331);
            simpleSink24=simpleSink();

            state._fsp--;

            adaptor.addChild(root_0, simpleSink24.getTree());
            EOF25=(Token)match(input,EOF,FOLLOW_EOF_in_sinkEof333); 
            EOF25_tree = (Object)adaptor.create(EOF25);
            adaptor.addChild(root_0, EOF25_tree);


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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:118:1: simpleSink : ( '[' multiSink ']' -> ^( MULTI multiSink ) | singleSink ( simpleSink )? -> ^( DECO singleSink ( simpleSink )? ) | '{' decoratedSink '}' -> ^( DECO decoratedSink ) | '<' failoverSink '>' -> ^( BACKUP failoverSink ) | rollSink -> rollSink );
    public final FlumeDeployParser.simpleSink_return simpleSink() throws RecognitionException {
        FlumeDeployParser.simpleSink_return retval = new FlumeDeployParser.simpleSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal26=null;
        Token char_literal28=null;
        Token char_literal31=null;
        Token char_literal33=null;
        Token char_literal34=null;
        Token char_literal36=null;
        FlumeDeployParser.multiSink_return multiSink27 = null;

        FlumeDeployParser.singleSink_return singleSink29 = null;

        FlumeDeployParser.simpleSink_return simpleSink30 = null;

        FlumeDeployParser.decoratedSink_return decoratedSink32 = null;

        FlumeDeployParser.failoverSink_return failoverSink35 = null;

        FlumeDeployParser.rollSink_return rollSink37 = null;


        Object char_literal26_tree=null;
        Object char_literal28_tree=null;
        Object char_literal31_tree=null;
        Object char_literal33_tree=null;
        Object char_literal34_tree=null;
        Object char_literal36_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_43=new RewriteRuleTokenStream(adaptor,"token 43");
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleSubtreeStream stream_multiSink=new RewriteRuleSubtreeStream(adaptor,"rule multiSink");
        RewriteRuleSubtreeStream stream_failoverSink=new RewriteRuleSubtreeStream(adaptor,"rule failoverSink");
        RewriteRuleSubtreeStream stream_singleSink=new RewriteRuleSubtreeStream(adaptor,"rule singleSink");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        RewriteRuleSubtreeStream stream_rollSink=new RewriteRuleSubtreeStream(adaptor,"rule rollSink");
        RewriteRuleSubtreeStream stream_decoratedSink=new RewriteRuleSubtreeStream(adaptor,"rule decoratedSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:118:12: ( '[' multiSink ']' -> ^( MULTI multiSink ) | singleSink ( simpleSink )? -> ^( DECO singleSink ( simpleSink )? ) | '{' decoratedSink '}' -> ^( DECO decoratedSink ) | '<' failoverSink '>' -> ^( BACKUP failoverSink ) | rollSink -> rollSink )
            int alt6=5;
            switch ( input.LA(1) ) {
            case 42:
                {
                alt6=1;
                }
                break;
            case Identifier:
                {
                alt6=2;
                }
                break;
            case 44:
                {
                alt6=3;
                }
                break;
            case 46:
                {
                alt6=4;
                }
                break;
            case 50:
                {
                alt6=5;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 0, input);

                throw nvae;
            }

            switch (alt6) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:118:14: '[' multiSink ']'
                    {
                    char_literal26=(Token)match(input,42,FOLLOW_42_in_simpleSink341);  
                    stream_42.add(char_literal26);

                    pushFollow(FOLLOW_multiSink_in_simpleSink343);
                    multiSink27=multiSink();

                    state._fsp--;

                    stream_multiSink.add(multiSink27.getTree());
                    char_literal28=(Token)match(input,43,FOLLOW_43_in_simpleSink345);  
                    stream_43.add(char_literal28);



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
                    // 118:34: -> ^( MULTI multiSink )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:118:37: ^( MULTI multiSink )
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:119:13: singleSink ( simpleSink )?
                    {
                    pushFollow(FOLLOW_singleSink_in_simpleSink370);
                    singleSink29=singleSink();

                    state._fsp--;

                    stream_singleSink.add(singleSink29.getTree());
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:119:24: ( simpleSink )?
                    int alt5=2;
                    alt5 = dfa5.predict(input);
                    switch (alt5) {
                        case 1 :
                            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:119:24: simpleSink
                            {
                            pushFollow(FOLLOW_simpleSink_in_simpleSink372);
                            simpleSink30=simpleSink();

                            state._fsp--;

                            stream_simpleSink.add(simpleSink30.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: singleSink, simpleSink
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 119:37: -> ^( DECO singleSink ( simpleSink )? )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:119:40: ^( DECO singleSink ( simpleSink )? )
                        {
                        Object root_1 = (Object)adaptor.nil();
                        root_1 = (Object)adaptor.becomeRoot((Object)adaptor.create(DECO, "DECO"), root_1);

                        adaptor.addChild(root_1, stream_singleSink.nextTree());
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:119:58: ( simpleSink )?
                        if ( stream_simpleSink.hasNext() ) {
                            adaptor.addChild(root_1, stream_simpleSink.nextTree());

                        }
                        stream_simpleSink.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:120:5: '{' decoratedSink '}'
                    {
                    char_literal31=(Token)match(input,44,FOLLOW_44_in_simpleSink392);  
                    stream_44.add(char_literal31);

                    pushFollow(FOLLOW_decoratedSink_in_simpleSink394);
                    decoratedSink32=decoratedSink();

                    state._fsp--;

                    stream_decoratedSink.add(decoratedSink32.getTree());
                    char_literal33=(Token)match(input,45,FOLLOW_45_in_simpleSink396);  
                    stream_45.add(char_literal33);



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
                    // 120:27: -> ^( DECO decoratedSink )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:120:30: ^( DECO decoratedSink )
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
                case 4 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:121:5: '<' failoverSink '>'
                    {
                    char_literal34=(Token)match(input,46,FOLLOW_46_in_simpleSink410);  
                    stream_46.add(char_literal34);

                    pushFollow(FOLLOW_failoverSink_in_simpleSink412);
                    failoverSink35=failoverSink();

                    state._fsp--;

                    stream_failoverSink.add(failoverSink35.getTree());
                    char_literal36=(Token)match(input,47,FOLLOW_47_in_simpleSink414);  
                    stream_47.add(char_literal36);



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
                    // 121:26: -> ^( BACKUP failoverSink )
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:121:29: ^( BACKUP failoverSink )
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
                case 5 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:122:13: rollSink
                    {
                    pushFollow(FOLLOW_rollSink_in_simpleSink436);
                    rollSink37=rollSink();

                    state._fsp--;

                    stream_rollSink.add(rollSink37.getTree());


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
                    // 122:37: -> rollSink
                    {
                        adaptor.addChild(root_0, stream_rollSink.nextTree());

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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:126:1: decoratedSink : singleSink '=>' sink -> singleSink sink ;
    public final FlumeDeployParser.decoratedSink_return decoratedSink() throws RecognitionException {
        FlumeDeployParser.decoratedSink_return retval = new FlumeDeployParser.decoratedSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token string_literal39=null;
        FlumeDeployParser.singleSink_return singleSink38 = null;

        FlumeDeployParser.sink_return sink40 = null;


        Object string_literal39_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleSubtreeStream stream_sink=new RewriteRuleSubtreeStream(adaptor,"rule sink");
        RewriteRuleSubtreeStream stream_singleSink=new RewriteRuleSubtreeStream(adaptor,"rule singleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:126:17: ( singleSink '=>' sink -> singleSink sink )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:126:20: singleSink '=>' sink
            {
            pushFollow(FOLLOW_singleSink_in_decoratedSink475);
            singleSink38=singleSink();

            state._fsp--;

            stream_singleSink.add(singleSink38.getTree());
            string_literal39=(Token)match(input,48,FOLLOW_48_in_decoratedSink477);  
            stream_48.add(string_literal39);

            pushFollow(FOLLOW_sink_in_decoratedSink479);
            sink40=sink();

            state._fsp--;

            stream_sink.add(sink40.getTree());


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
            // 126:44: -> singleSink sink
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:127:1: multiSink : simpleSink ( ',' simpleSink )* -> ( simpleSink )* ;
    public final FlumeDeployParser.multiSink_return multiSink() throws RecognitionException {
        FlumeDeployParser.multiSink_return retval = new FlumeDeployParser.multiSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal42=null;
        FlumeDeployParser.simpleSink_return simpleSink41 = null;

        FlumeDeployParser.simpleSink_return simpleSink43 = null;


        Object char_literal42_tree=null;
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:127:17: ( simpleSink ( ',' simpleSink )* -> ( simpleSink )* )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:127:20: simpleSink ( ',' simpleSink )*
            {
            pushFollow(FOLLOW_simpleSink_in_multiSink502);
            simpleSink41=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink41.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:127:31: ( ',' simpleSink )*
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( (LA7_0==41) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:127:32: ',' simpleSink
            	    {
            	    char_literal42=(Token)match(input,41,FOLLOW_41_in_multiSink505);  
            	    stream_41.add(char_literal42);

            	    pushFollow(FOLLOW_simpleSink_in_multiSink507);
            	    simpleSink43=simpleSink();

            	    state._fsp--;

            	    stream_simpleSink.add(simpleSink43.getTree());

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
            // 127:50: -> ( simpleSink )*
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:127:53: ( simpleSink )*
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:128:1: failoverSink : simpleSink ( '?' simpleSink )+ -> ( simpleSink )+ ;
    public final FlumeDeployParser.failoverSink_return failoverSink() throws RecognitionException {
        FlumeDeployParser.failoverSink_return retval = new FlumeDeployParser.failoverSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal45=null;
        FlumeDeployParser.simpleSink_return simpleSink44 = null;

        FlumeDeployParser.simpleSink_return simpleSink46 = null;


        Object char_literal45_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:128:17: ( simpleSink ( '?' simpleSink )+ -> ( simpleSink )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:128:20: simpleSink ( '?' simpleSink )+
            {
            pushFollow(FOLLOW_simpleSink_in_failoverSink527);
            simpleSink44=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink44.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:128:31: ( '?' simpleSink )+
            int cnt8=0;
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==49) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:128:32: '?' simpleSink
            	    {
            	    char_literal45=(Token)match(input,49,FOLLOW_49_in_failoverSink530);  
            	    stream_49.add(char_literal45);

            	    pushFollow(FOLLOW_simpleSink_in_failoverSink532);
            	    simpleSink46=simpleSink();

            	    state._fsp--;

            	    stream_simpleSink.add(simpleSink46.getTree());

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
            // 128:49: -> ( simpleSink )+
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

    public static class rollSink_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "rollSink"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:129:1: rollSink : 'roll' args '{' simpleSink '}' -> ^( ROLL simpleSink args ) ;
    public final FlumeDeployParser.rollSink_return rollSink() throws RecognitionException {
        FlumeDeployParser.rollSink_return retval = new FlumeDeployParser.rollSink_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token string_literal47=null;
        Token char_literal49=null;
        Token char_literal51=null;
        FlumeDeployParser.args_return args48 = null;

        FlumeDeployParser.simpleSink_return simpleSink50 = null;


        Object string_literal47_tree=null;
        Object char_literal49_tree=null;
        Object char_literal51_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        RewriteRuleSubtreeStream stream_simpleSink=new RewriteRuleSubtreeStream(adaptor,"rule simpleSink");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:129:17: ( 'roll' args '{' simpleSink '}' -> ^( ROLL simpleSink args ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:129:20: 'roll' args '{' simpleSink '}'
            {
            string_literal47=(Token)match(input,50,FOLLOW_50_in_rollSink555);  
            stream_50.add(string_literal47);

            pushFollow(FOLLOW_args_in_rollSink557);
            args48=args();

            state._fsp--;

            stream_args.add(args48.getTree());
            char_literal49=(Token)match(input,44,FOLLOW_44_in_rollSink559);  
            stream_44.add(char_literal49);

            pushFollow(FOLLOW_simpleSink_in_rollSink561);
            simpleSink50=simpleSink();

            state._fsp--;

            stream_simpleSink.add(simpleSink50.getTree());
            char_literal51=(Token)match(input,45,FOLLOW_45_in_rollSink563);  
            stream_45.add(char_literal51);



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
            // 130:35: -> ^( ROLL simpleSink args )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:130:38: ^( ROLL simpleSink args )
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

    public static class args_return extends ParserRuleReturnScope {
        Object tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "args"
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:1: args : ( '(' ( arglist ( ',' kwarglist )? ) ')' -> arglist ( kwarglist )? | '(' kwarglist ')' -> ( kwarglist )? | '(' ')' ->);
    public final FlumeDeployParser.args_return args() throws RecognitionException {
        FlumeDeployParser.args_return retval = new FlumeDeployParser.args_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal52=null;
        Token char_literal54=null;
        Token char_literal56=null;
        Token char_literal57=null;
        Token char_literal59=null;
        Token char_literal60=null;
        Token char_literal61=null;
        FlumeDeployParser.arglist_return arglist53 = null;

        FlumeDeployParser.kwarglist_return kwarglist55 = null;

        FlumeDeployParser.kwarglist_return kwarglist58 = null;


        Object char_literal52_tree=null;
        Object char_literal54_tree=null;
        Object char_literal56_tree=null;
        Object char_literal57_tree=null;
        Object char_literal59_tree=null;
        Object char_literal60_tree=null;
        Object char_literal61_tree=null;
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleTokenStream stream_52=new RewriteRuleTokenStream(adaptor,"token 52");
        RewriteRuleSubtreeStream stream_arglist=new RewriteRuleSubtreeStream(adaptor,"rule arglist");
        RewriteRuleSubtreeStream stream_kwarglist=new RewriteRuleSubtreeStream(adaptor,"rule kwarglist");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:9: ( '(' ( arglist ( ',' kwarglist )? ) ')' -> arglist ( kwarglist )? | '(' kwarglist ')' -> ( kwarglist )? | '(' ')' ->)
            int alt10=3;
            alt10 = dfa10.predict(input);
            switch (alt10) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:11: '(' ( arglist ( ',' kwarglist )? ) ')'
                    {
                    char_literal52=(Token)match(input,51,FOLLOW_51_in_args618);  
                    stream_51.add(char_literal52);

                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:15: ( arglist ( ',' kwarglist )? )
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:17: arglist ( ',' kwarglist )?
                    {
                    pushFollow(FOLLOW_arglist_in_args622);
                    arglist53=arglist();

                    state._fsp--;

                    stream_arglist.add(arglist53.getTree());
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:25: ( ',' kwarglist )?
                    int alt9=2;
                    int LA9_0 = input.LA(1);

                    if ( (LA9_0==41) ) {
                        alt9=1;
                    }
                    switch (alt9) {
                        case 1 :
                            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:26: ',' kwarglist
                            {
                            char_literal54=(Token)match(input,41,FOLLOW_41_in_args625);  
                            stream_41.add(char_literal54);

                            pushFollow(FOLLOW_kwarglist_in_args627);
                            kwarglist55=kwarglist();

                            state._fsp--;

                            stream_kwarglist.add(kwarglist55.getTree());

                            }
                            break;

                    }


                    }

                    char_literal56=(Token)match(input,52,FOLLOW_52_in_args634);  
                    stream_52.add(char_literal56);



                    // AST REWRITE
                    // elements: kwarglist, arglist
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (Object)adaptor.nil();
                    // 132:49: -> arglist ( kwarglist )?
                    {
                        adaptor.addChild(root_0, stream_arglist.nextTree());
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:132:60: ( kwarglist )?
                        if ( stream_kwarglist.hasNext() ) {
                            adaptor.addChild(root_0, stream_kwarglist.nextTree());

                        }
                        stream_kwarglist.reset();

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:133:11: '(' kwarglist ')'
                    {
                    char_literal57=(Token)match(input,51,FOLLOW_51_in_args653);  
                    stream_51.add(char_literal57);

                    pushFollow(FOLLOW_kwarglist_in_args655);
                    kwarglist58=kwarglist();

                    state._fsp--;

                    stream_kwarglist.add(kwarglist58.getTree());
                    char_literal59=(Token)match(input,52,FOLLOW_52_in_args657);  
                    stream_52.add(char_literal59);



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
                    // 133:29: -> ( kwarglist )?
                    {
                        // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:133:32: ( kwarglist )?
                        if ( stream_kwarglist.hasNext() ) {
                            adaptor.addChild(root_0, stream_kwarglist.nextTree());

                        }
                        stream_kwarglist.reset();

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:134:11: '(' ')'
                    {
                    char_literal60=(Token)match(input,51,FOLLOW_51_in_args675);  
                    stream_51.add(char_literal60);

                    char_literal61=(Token)match(input,52,FOLLOW_52_in_args677);  
                    stream_52.add(char_literal61);



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
                    // 134:19: ->
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:137:1: arglist : literal ( ',' literal )* -> ( literal )+ ;
    public final FlumeDeployParser.arglist_return arglist() throws RecognitionException {
        FlumeDeployParser.arglist_return retval = new FlumeDeployParser.arglist_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal63=null;
        FlumeDeployParser.literal_return literal62 = null;

        FlumeDeployParser.literal_return literal64 = null;


        Object char_literal63_tree=null;
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_literal=new RewriteRuleSubtreeStream(adaptor,"rule literal");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:137:9: ( literal ( ',' literal )* -> ( literal )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:137:11: literal ( ',' literal )*
            {
            pushFollow(FOLLOW_literal_in_arglist697);
            literal62=literal();

            state._fsp--;

            stream_literal.add(literal62.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:137:19: ( ',' literal )*
            loop11:
            do {
                int alt11=2;
                alt11 = dfa11.predict(input);
                switch (alt11) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:137:20: ',' literal
            	    {
            	    char_literal63=(Token)match(input,41,FOLLOW_41_in_arglist700);  
            	    stream_41.add(char_literal63);

            	    pushFollow(FOLLOW_literal_in_arglist702);
            	    literal64=literal();

            	    state._fsp--;

            	    stream_literal.add(literal64.getTree());

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
            // 137:34: -> ( literal )+
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:1: kwarglist : kwarg ( ',' kwarg )* -> ( kwarg )+ ;
    public final FlumeDeployParser.kwarglist_return kwarglist() throws RecognitionException {
        FlumeDeployParser.kwarglist_return retval = new FlumeDeployParser.kwarglist_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token char_literal66=null;
        FlumeDeployParser.kwarg_return kwarg65 = null;

        FlumeDeployParser.kwarg_return kwarg67 = null;


        Object char_literal66_tree=null;
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_kwarg=new RewriteRuleSubtreeStream(adaptor,"rule kwarg");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:11: ( kwarg ( ',' kwarg )* -> ( kwarg )+ )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:13: kwarg ( ',' kwarg )*
            {
            pushFollow(FOLLOW_kwarg_in_kwarglist718);
            kwarg65=kwarg();

            state._fsp--;

            stream_kwarg.add(kwarg65.getTree());
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:19: ( ',' kwarg )*
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( (LA12_0==41) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:139:20: ',' kwarg
            	    {
            	    char_literal66=(Token)match(input,41,FOLLOW_41_in_kwarglist721);  
            	    stream_41.add(char_literal66);

            	    pushFollow(FOLLOW_kwarg_in_kwarglist723);
            	    kwarg67=kwarg();

            	    state._fsp--;

            	    stream_kwarg.add(kwarg67.getTree());

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
            // 139:32: -> ( kwarg )+
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:141:1: kwarg : Identifier '=' literal -> ^( KWARG Identifier literal ) ;
    public final FlumeDeployParser.kwarg_return kwarg() throws RecognitionException {
        FlumeDeployParser.kwarg_return retval = new FlumeDeployParser.kwarg_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token Identifier68=null;
        Token char_literal69=null;
        FlumeDeployParser.literal_return literal70 = null;


        Object Identifier68_tree=null;
        Object char_literal69_tree=null;
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleSubtreeStream stream_literal=new RewriteRuleSubtreeStream(adaptor,"rule literal");
        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:141:9: ( Identifier '=' literal -> ^( KWARG Identifier literal ) )
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:141:13: Identifier '=' literal
            {
            Identifier68=(Token)match(input,Identifier,FOLLOW_Identifier_in_kwarg742);  
            stream_Identifier.add(Identifier68);

            char_literal69=(Token)match(input,53,FOLLOW_53_in_kwarg744);  
            stream_53.add(char_literal69);

            pushFollow(FOLLOW_literal_in_kwarg746);
            literal70=literal();

            state._fsp--;

            stream_literal.add(literal70.getTree());


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
            // 141:36: -> ^( KWARG Identifier literal )
            {
                // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:141:39: ^( KWARG Identifier literal )
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
    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:144:1: literal : ( integerLiteral | StringLiteral -> ^( STRING StringLiteral ) | booleanLiteral | FloatingPointLiteral -> ^( FLOAT FloatingPointLiteral ) );
    public final FlumeDeployParser.literal_return literal() throws RecognitionException {
        FlumeDeployParser.literal_return retval = new FlumeDeployParser.literal_return();
        retval.start = input.LT(1);

        Object root_0 = null;

        Token StringLiteral72=null;
        Token FloatingPointLiteral74=null;
        FlumeDeployParser.integerLiteral_return integerLiteral71 = null;

        FlumeDeployParser.booleanLiteral_return booleanLiteral73 = null;


        Object StringLiteral72_tree=null;
        Object FloatingPointLiteral74_tree=null;
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleTokenStream stream_FloatingPointLiteral=new RewriteRuleTokenStream(adaptor,"token FloatingPointLiteral");

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:145:5: ( integerLiteral | StringLiteral -> ^( STRING StringLiteral ) | booleanLiteral | FloatingPointLiteral -> ^( FLOAT FloatingPointLiteral ) )
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
            case 54:
            case 55:
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:145:9: integerLiteral
                    {
                    root_0 = (Object)adaptor.nil();

                    pushFollow(FOLLOW_integerLiteral_in_literal773);
                    integerLiteral71=integerLiteral();

                    state._fsp--;

                    adaptor.addChild(root_0, integerLiteral71.getTree());

                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:146:9: StringLiteral
                    {
                    StringLiteral72=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_literal783);  
                    stream_StringLiteral.add(StringLiteral72);



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

                    pushFollow(FOLLOW_booleanLiteral_in_literal802);
                    booleanLiteral73=booleanLiteral();

                    state._fsp--;

                    adaptor.addChild(root_0, booleanLiteral73.getTree());

                    }
                    break;
                case 4 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:148:9: FloatingPointLiteral
                    {
                    FloatingPointLiteral74=(Token)match(input,FloatingPointLiteral,FOLLOW_FloatingPointLiteral_in_literal812);  
                    stream_FloatingPointLiteral.add(FloatingPointLiteral74);



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

        Token HexLiteral75=null;
        Token OctalLiteral76=null;
        Token DecimalLiteral77=null;

        Object HexLiteral75_tree=null;
        Object OctalLiteral76_tree=null;
        Object DecimalLiteral77_tree=null;
        RewriteRuleTokenStream stream_HexLiteral=new RewriteRuleTokenStream(adaptor,"token HexLiteral");
        RewriteRuleTokenStream stream_DecimalLiteral=new RewriteRuleTokenStream(adaptor,"token DecimalLiteral");
        RewriteRuleTokenStream stream_OctalLiteral=new RewriteRuleTokenStream(adaptor,"token OctalLiteral");

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:151:5: ( HexLiteral -> ^( HEX HexLiteral ) | OctalLiteral -> ^( OCT OctalLiteral ) | DecimalLiteral -> ^( DEC DecimalLiteral ) )
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
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:151:9: HexLiteral
                    {
                    HexLiteral75=(Token)match(input,HexLiteral,FOLLOW_HexLiteral_in_integerLiteral838);  
                    stream_HexLiteral.add(HexLiteral75);



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
                    OctalLiteral76=(Token)match(input,OctalLiteral,FOLLOW_OctalLiteral_in_integerLiteral857);  
                    stream_OctalLiteral.add(OctalLiteral76);



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
                    DecimalLiteral77=(Token)match(input,DecimalLiteral,FOLLOW_DecimalLiteral_in_integerLiteral876);  
                    stream_DecimalLiteral.add(DecimalLiteral77);



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

        Token string_literal78=null;
        Token string_literal79=null;

        Object string_literal78_tree=null;
        Object string_literal79_tree=null;
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleTokenStream stream_54=new RewriteRuleTokenStream(adaptor,"token 54");

        try {
            // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:157:5: ( 'true' -> ^( BOOL 'true' ) | 'false' -> ^( BOOL 'false' ) )
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( (LA15_0==54) ) {
                alt15=1;
            }
            else if ( (LA15_0==55) ) {
                alt15=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 15, 0, input);

                throw nvae;
            }
            switch (alt15) {
                case 1 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:157:9: 'true'
                    {
                    string_literal78=(Token)match(input,54,FOLLOW_54_in_booleanLiteral908);  
                    stream_54.add(string_literal78);



                    // AST REWRITE
                    // elements: 54
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

                        adaptor.addChild(root_1, stream_54.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // /home/jon/flume/src/antlr/com/cloudera/flume/conf/FlumeDeploy.g:158:9: 'false'
                    {
                    string_literal79=(Token)match(input,55,FOLLOW_55_in_booleanLiteral928);  
                    stream_55.add(string_literal79);



                    // AST REWRITE
                    // elements: 55
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

                        adaptor.addChild(root_1, stream_55.nextNode());

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


    protected DFA4 dfa4 = new DFA4(this);
    protected DFA5 dfa5 = new DFA5(this);
    protected DFA10 dfa10 = new DFA10(this);
    protected DFA11 dfa11 = new DFA11(this);
    static final String DFA4_eotS =
        "\17\uffff";
    static final String DFA4_eofS =
        "\1\2\16\uffff";
    static final String DFA4_minS =
        "\1\23\16\uffff";
    static final String DFA4_maxS =
        "\1\63\16\uffff";
    static final String DFA4_acceptS =
        "\1\uffff\1\1\1\2\14\uffff";
    static final String DFA4_specialS =
        "\17\uffff}>";
    static final String[] DFA4_transitionS = {
            "\1\2\24\uffff\13\2\1\1",
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
            "",
            "",
            "",
            ""
    };

    static final short[] DFA4_eot = DFA.unpackEncodedString(DFA4_eotS);
    static final short[] DFA4_eof = DFA.unpackEncodedString(DFA4_eofS);
    static final char[] DFA4_min = DFA.unpackEncodedStringToUnsignedChars(DFA4_minS);
    static final char[] DFA4_max = DFA.unpackEncodedStringToUnsignedChars(DFA4_maxS);
    static final short[] DFA4_accept = DFA.unpackEncodedString(DFA4_acceptS);
    static final short[] DFA4_special = DFA.unpackEncodedString(DFA4_specialS);
    static final short[][] DFA4_transition;

    static {
        int numStates = DFA4_transitionS.length;
        DFA4_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA4_transition[i] = DFA.unpackEncodedString(DFA4_transitionS[i]);
        }
    }

    class DFA4 extends DFA {

        public DFA4(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 4;
            this.eot = DFA4_eot;
            this.eof = DFA4_eof;
            this.min = DFA4_min;
            this.max = DFA4_max;
            this.accept = DFA4_accept;
            this.special = DFA4_special;
            this.transition = DFA4_transition;
        }
        public String getDescription() {
            return "114:25: ( args )?";
        }
    }
    static final String DFA5_eotS =
        "\15\uffff";
    static final String DFA5_eofS =
        "\1\6\14\uffff";
    static final String DFA5_minS =
        "\1\23\14\uffff";
    static final String DFA5_maxS =
        "\1\62\14\uffff";
    static final String DFA5_acceptS =
        "\1\uffff\1\1\4\uffff\1\2\6\uffff";
    static final String DFA5_specialS =
        "\15\uffff}>";
    static final String[] DFA5_transitionS = {
            "\1\1\24\uffff\2\6\1\1\1\6\1\1\1\6\1\1\1\6\1\uffff\1\6\1\1",
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
            return "119:24: ( simpleSink )?";
        }
    }
    static final String DFA10_eotS =
        "\13\uffff";
    static final String DFA10_eofS =
        "\13\uffff";
    static final String DFA10_minS =
        "\1\63\1\23\11\uffff";
    static final String DFA10_maxS =
        "\1\63\1\67\11\uffff";
    static final String DFA10_acceptS =
        "\2\uffff\1\3\1\2\1\1\6\uffff";
    static final String DFA10_specialS =
        "\13\uffff}>";
    static final String[] DFA10_transitionS = {
            "\1\1",
            "\1\3\1\uffff\5\4\32\uffff\1\2\1\uffff\2\4",
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
            return "132:1: args : ( '(' ( arglist ( ',' kwarglist )? ) ')' -> arglist ( kwarglist )? | '(' kwarglist ')' -> ( kwarglist )? | '(' ')' ->);";
        }
    }
    static final String DFA11_eotS =
        "\13\uffff";
    static final String DFA11_eofS =
        "\13\uffff";
    static final String DFA11_minS =
        "\1\51\1\23\11\uffff";
    static final String DFA11_maxS =
        "\1\64\1\67\11\uffff";
    static final String DFA11_acceptS =
        "\2\uffff\1\2\1\uffff\1\1\6\uffff";
    static final String DFA11_specialS =
        "\13\uffff}>";
    static final String[] DFA11_transitionS = {
            "\1\1\12\uffff\1\2",
            "\1\2\1\uffff\5\4\34\uffff\2\4",
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
            return "()* loopback of 137:19: ( ',' literal )*";
        }
    }
 

    public static final BitSet FOLLOW_def_in_deflist135 = new BitSet(new long[]{0x0000000000180000L});
    public static final BitSet FOLLOW_EOF_in_deflist138 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_host_in_def148 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_38_in_def150 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_source_in_def152 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_39_in_def154 = new BitSet(new long[]{0x0004540000080000L});
    public static final BitSet FOLLOW_sink_in_def156 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_40_in_def159 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_host0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_source_in_connection195 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_39_in_connection197 = new BitSet(new long[]{0x0004540000080000L});
    public static final BitSet FOLLOW_sink_in_connection199 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSource_in_source221 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_source_in_sourceEof235 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_sourceEof237 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_singleSource250 = new BitSet(new long[]{0x0008000000000002L});
    public static final BitSet FOLLOW_args_in_singleSource252 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSource_in_multiSource271 = new BitSet(new long[]{0x0000020000000002L});
    public static final BitSet FOLLOW_41_in_multiSource274 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_singleSource_in_multiSource276 = new BitSet(new long[]{0x0000020000000002L});
    public static final BitSet FOLLOW_simpleSink_in_sink295 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_singleSink307 = new BitSet(new long[]{0x0008000000000002L});
    public static final BitSet FOLLOW_args_in_singleSink309 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_simpleSink_in_sinkEof331 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_sinkEof333 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_42_in_simpleSink341 = new BitSet(new long[]{0x0004540000080000L});
    public static final BitSet FOLLOW_multiSink_in_simpleSink343 = new BitSet(new long[]{0x0000080000000000L});
    public static final BitSet FOLLOW_43_in_simpleSink345 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSink_in_simpleSink370 = new BitSet(new long[]{0x0004540000080002L});
    public static final BitSet FOLLOW_simpleSink_in_simpleSink372 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_44_in_simpleSink392 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_decoratedSink_in_simpleSink394 = new BitSet(new long[]{0x0000200000000000L});
    public static final BitSet FOLLOW_45_in_simpleSink396 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_46_in_simpleSink410 = new BitSet(new long[]{0x0004540000080000L});
    public static final BitSet FOLLOW_failoverSink_in_simpleSink412 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_simpleSink414 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rollSink_in_simpleSink436 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_singleSink_in_decoratedSink475 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_decoratedSink477 = new BitSet(new long[]{0x0004540000080000L});
    public static final BitSet FOLLOW_sink_in_decoratedSink479 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_simpleSink_in_multiSink502 = new BitSet(new long[]{0x0000020000000002L});
    public static final BitSet FOLLOW_41_in_multiSink505 = new BitSet(new long[]{0x0004540000080000L});
    public static final BitSet FOLLOW_simpleSink_in_multiSink507 = new BitSet(new long[]{0x0000020000000002L});
    public static final BitSet FOLLOW_simpleSink_in_failoverSink527 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_failoverSink530 = new BitSet(new long[]{0x0004540000080000L});
    public static final BitSet FOLLOW_simpleSink_in_failoverSink532 = new BitSet(new long[]{0x0002000000000002L});
    public static final BitSet FOLLOW_50_in_rollSink555 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_args_in_rollSink557 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_44_in_rollSink559 = new BitSet(new long[]{0x0004540000080000L});
    public static final BitSet FOLLOW_simpleSink_in_rollSink561 = new BitSet(new long[]{0x0000200000000000L});
    public static final BitSet FOLLOW_45_in_rollSink563 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_51_in_args618 = new BitSet(new long[]{0x00C0000003E00000L});
    public static final BitSet FOLLOW_arglist_in_args622 = new BitSet(new long[]{0x0010020000000000L});
    public static final BitSet FOLLOW_41_in_args625 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_kwarglist_in_args627 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_52_in_args634 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_51_in_args653 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_kwarglist_in_args655 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_52_in_args657 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_51_in_args675 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_52_in_args677 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_arglist697 = new BitSet(new long[]{0x0000020000000002L});
    public static final BitSet FOLLOW_41_in_arglist700 = new BitSet(new long[]{0x00C0000003E00000L});
    public static final BitSet FOLLOW_literal_in_arglist702 = new BitSet(new long[]{0x0000020000000002L});
    public static final BitSet FOLLOW_kwarg_in_kwarglist718 = new BitSet(new long[]{0x0000020000000002L});
    public static final BitSet FOLLOW_41_in_kwarglist721 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_kwarg_in_kwarglist723 = new BitSet(new long[]{0x0000020000000002L});
    public static final BitSet FOLLOW_Identifier_in_kwarg742 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_kwarg744 = new BitSet(new long[]{0x00C0000003E00000L});
    public static final BitSet FOLLOW_literal_in_kwarg746 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_integerLiteral_in_literal773 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_literal783 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_booleanLiteral_in_literal802 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FloatingPointLiteral_in_literal812 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HexLiteral_in_integerLiteral838 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_OctalLiteral_in_integerLiteral857 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DecimalLiteral_in_integerLiteral876 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_54_in_booleanLiteral908 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_booleanLiteral928 = new BitSet(new long[]{0x0000000000000002L});

}