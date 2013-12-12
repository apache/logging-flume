/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.solr.morphline;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.solr.DocumentLoader;
import org.kitesdk.morphline.solr.SolrLocator;
import org.kitesdk.morphline.solr.SolrMorphlineContext;
import org.kitesdk.morphline.solr.SolrServerDocumentLoader;
import org.kitesdk.morphline.solr.TestEmbeddedSolrServer;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Files;

public class TestMorphlineSolrSink extends SolrTestCaseJ4 {

  private EmbeddedSource source;
  private SolrServer solrServer;
  private MorphlineSink sink;
  private Map<String,Integer> expectedRecords;

  private File tmpFile;
  private static final boolean TEST_WITH_EMBEDDED_SOLR_SERVER = true;
  private static final String EXTERNAL_SOLR_SERVER_URL = System.getProperty("externalSolrServer");
//private static final String EXTERNAL_SOLR_SERVER_URL = "http://127.0.0.1:8983/solr";
  private static final String RESOURCES_DIR = "target/test-classes";
//private static final String RESOURCES_DIR = "src/test/resources";
  private static final AtomicInteger SEQ_NUM = new AtomicInteger();
  private static final AtomicInteger SEQ_NUM2 = new AtomicInteger();
  private static final Logger LOGGER = LoggerFactory.getLogger(TestMorphlineSolrSink.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(
        RESOURCES_DIR + "/solr/collection1/conf/solrconfig.xml", 
        RESOURCES_DIR + "/solr/collection1/conf/schema.xml",
        RESOURCES_DIR + "/solr"
        );
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    String path = RESOURCES_DIR + "/test-documents";
    expectedRecords = new HashMap();
    expectedRecords.put(path + "/sample-statuses-20120906-141433.avro", 2);
    expectedRecords.put(path + "/sample-statuses-20120906-141433", 2);
    expectedRecords.put(path + "/sample-statuses-20120906-141433.gz", 2);
    expectedRecords.put(path + "/sample-statuses-20120906-141433.bz2", 2);
    expectedRecords.put(path + "/cars.csv", 5);
    expectedRecords.put(path + "/cars.csv.gz", 5);
    expectedRecords.put(path + "/cars.tar.gz", 4);
    expectedRecords.put(path + "/cars.tsv", 5);
    expectedRecords.put(path + "/cars.ssv", 5);

    final Map<String, String> context = new HashMap();
    
    if (EXTERNAL_SOLR_SERVER_URL != null) {
      throw new UnsupportedOperationException();
      //solrServer = new ConcurrentUpdateSolrServer(EXTERNAL_SOLR_SERVER_URL, 2, 2);
      //solrServer = new SafeConcurrentUpdateSolrServer(EXTERNAL_SOLR_SERVER_URL, 2, 2);
      //solrServer = new HttpSolrServer(EXTERNAL_SOLR_SERVER_URL);
    } else {
      if (TEST_WITH_EMBEDDED_SOLR_SERVER) {
        solrServer = new TestEmbeddedSolrServer(h.getCoreContainer(), "");
      } else {
        throw new RuntimeException("Not yet implemented");
        //solrServer = new TestSolrServer(getSolrServer());
      }
    }

    Map<String, String> channelContext = new HashMap();
    channelContext.put("capacity", "1000000");
    channelContext.put("keep-alive", "0"); // for faster tests
    Channel channel = new MemoryChannel();
    channel.setName(channel.getClass().getName() + SEQ_NUM.getAndIncrement());
    Configurables.configure(channel, new Context(channelContext));
 
    class MySolrSink extends MorphlineSolrSink {
      public MySolrSink(MorphlineHandlerImpl indexer) {
        super(indexer);
      }
    }
    
    int batchSize = SEQ_NUM2.incrementAndGet() % 2 == 0 ? 100 : 1;
    DocumentLoader testServer = new SolrServerDocumentLoader(solrServer, batchSize);
    MorphlineContext solrMorphlineContext = new SolrMorphlineContext.Builder()
      .setDocumentLoader(testServer)
      .setExceptionHandler(new FaultTolerance(false, false, SolrServerException.class.getName()))
      .setMetricRegistry(new MetricRegistry()).build();
    
    MorphlineHandlerImpl impl = new MorphlineHandlerImpl();
    impl.setMorphlineContext(solrMorphlineContext);
    
    class MySolrLocator extends SolrLocator { // trick to access protected ctor
      public MySolrLocator(MorphlineContext indexer) {
        super(indexer);
      }
    }

    SolrLocator locator = new MySolrLocator(solrMorphlineContext);
    locator.setSolrHomeDir(testSolrHome + "/collection1");
    String str1 = "SOLR_LOCATOR : " + locator.toString();
    //File solrLocatorFile = new File("target/test-classes/test-morphlines/solrLocator.conf");
    //String str1 = Files.toString(solrLocatorFile, Charsets.UTF_8);
    File morphlineFile = new File("target/test-classes/test-morphlines/solrCellDocumentTypes.conf");
    String str2 = Files.toString(morphlineFile, Charsets.UTF_8);
    tmpFile = File.createTempFile("morphline", ".conf");
    tmpFile.deleteOnExit();
    Files.write(str1 + "\n" + str2, tmpFile, Charsets.UTF_8);    
    context.put("morphlineFile", tmpFile.getPath());

    impl.configure(new Context(context));
    sink = new MySolrSink(impl);
    sink.setName(sink.getClass().getName() + SEQ_NUM.getAndIncrement());
    sink.configure(new Context(context));
    sink.setChannel(channel);
    sink.start();
    
    source = new EmbeddedSource(sink);    
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(Collections.singletonList(channel));
    ChannelProcessor chp = new ChannelProcessor(rcs);
    Context chpContext = new Context();
    chpContext.put("interceptors", "uuidinterceptor");
    chpContext.put("interceptors.uuidinterceptor.type", UUIDInterceptor.Builder.class.getName());
    chp.configure(chpContext);
    source.setChannelProcessor(chp);
    
    deleteAllDocuments();
  }
  
  private void deleteAllDocuments() throws SolrServerException, IOException {
    SolrServer s = solrServer;
    s.deleteByQuery("*:*"); // delete everything!
    s.commit();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    try {
      if (source != null) {
        source.stop();
        source = null;
      }
      if (sink != null) {
        sink.stop();
        sink = null;
      }
      if (tmpFile != null) {
        tmpFile.delete();
      }
    } finally {
      solrServer = null;
      expectedRecords = null;
      super.tearDown();
    }
  }

  @Test
  public void testDocumentTypes() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testBMPfp.txt",
        path + "/boilerplate.html",
        path + "/NullHeader.docx",
        path + "/testWORD_various.doc",          
        path + "/testPDF.pdf",
        path + "/testJPEG_EXIF.jpg",
        path + "/testXML.xml",          
//        path + "/cars.csv",
//        path + "/cars.tsv",
//        path + "/cars.ssv",
//        path + "/cars.csv.gz",
//        path + "/cars.tar.gz",
        path + "/sample-statuses-20120906-141433.avro",
        path + "/sample-statuses-20120906-141433",
        path + "/sample-statuses-20120906-141433.gz",
        path + "/sample-statuses-20120906-141433.bz2",
    };
    testDocumentTypesInternal(files);
  }

  @Test
  public void testDocumentTypes2() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testPPT_various.ppt",
        path + "/testPPT_various.pptx",        
        path + "/testEXCEL.xlsx",
        path + "/testEXCEL.xls", 
        path + "/testPages.pages", 
        path + "/testNumbers.numbers", 
        path + "/testKeynote.key",
        
        path + "/testRTFVarious.rtf", 
        path + "/complex.mbox", 
        path + "/test-outlook.msg", 
        path + "/testEMLX.emlx",
//        path + "/testRFC822",  
        path + "/rsstest.rss", 
//        path + "/testDITA.dita", 
        
        path + "/testMP3i18n.mp3", 
        path + "/testAIFF.aif", 
        path + "/testFLAC.flac", 
//        path + "/testFLAC.oga", 
//        path + "/testVORBIS.ogg",  
        path + "/testMP4.m4a", 
        path + "/testWAV.wav", 
//        path + "/testWMA.wma", 
        
        path + "/testFLV.flv", 
//        path + "/testWMV.wmv", 
        
        path + "/testBMP.bmp", 
        path + "/testPNG.png", 
        path + "/testPSD.psd",        
        path + "/testSVG.svg",  
        path + "/testTIFF.tif",     

//        path + "/test-documents.7z", 
//        path + "/test-documents.cpio",
//        path + "/test-documents.tar", 
//        path + "/test-documents.tbz2", 
//        path + "/test-documents.tgz",
//        path + "/test-documents.zip",
//        path + "/test-zip-of-zip.zip",
//        path + "/testJAR.jar",
        
//        path + "/testKML.kml", 
//        path + "/testRDF.rdf", 
        path + "/testTrueType.ttf", 
        path + "/testVISIO.vsd",
//        path + "/testWAR.war", 
//        path + "/testWindows-x86-32.exe",
//        path + "/testWINMAIL.dat", 
//        path + "/testWMF.wmf", 
    };   
    testDocumentTypesInternal(files);
  }

  @Test
  public void testAvroRoundTrip() throws Exception {
    String file = RESOURCES_DIR + "/test-documents" + "/sample-statuses-20120906-141433.avro";
    testDocumentTypesInternal(file);
    QueryResponse rsp = query("*:*");
    Iterator<SolrDocument> iter = rsp.getResults().iterator();
    ListMultimap<String, String> expectedFieldValues;
    expectedFieldValues = ImmutableListMultimap.of("id", "1234567890", "text", "sample tweet one", "user_screen_name", "fake_user1");
    assertEquals(expectedFieldValues, next(iter));
    expectedFieldValues = ImmutableListMultimap.of("id", "2345678901", "text", "sample tweet two", "user_screen_name", "fake_user2");  
    assertEquals(expectedFieldValues, next(iter));
    assertFalse(iter.hasNext());
  }
  
  private ListMultimap<String, Object> next(Iterator<SolrDocument> iter) {
    SolrDocument doc = iter.next();
    Record record = toRecord(doc);
    record.removeAll("_version_"); // the values of this field are unknown and internal to solr
    return record.getFields();    
  }
  
  private Record toRecord(SolrDocument doc) {
    Record record = new Record();
    for (String key : doc.keySet()) {
      record.getFields().replaceValues(key, doc.getFieldValues(key));        
    }
    return record;
  }
  
  private void testDocumentTypesInternal(String... files) throws Exception {
    int numDocs = 0;
    long startTime = System.currentTimeMillis();
    
    assertEquals(numDocs, queryResultSetSize("*:*"));      
//  assertQ(req("*:*"), "//*[@numFound='0']");
    for (int i = 0; i < 1; i++) {      
      for (String file : files) {
        File f = new File(file);
        byte[] body = Files.toByteArray(f);
        Event event = EventBuilder.withBody(body);
        event.getHeaders().put(Fields.ATTACHMENT_NAME, f.getName());
        load(event);
        Integer count = expectedRecords.get(file);
        if (count != null) {
          numDocs += count;
        } else {
          numDocs++;
        }
        assertEquals(numDocs, queryResultSetSize("*:*"));
      }
      LOGGER.trace("iter: {}", i);
    }
    LOGGER.trace("all done with put at {}", System.currentTimeMillis() - startTime);
    assertEquals(numDocs, queryResultSetSize("*:*"));
    LOGGER.trace("sink: ", sink);
  }

//  @Test
  public void benchmarkDocumentTypes() throws Exception {
    int iters = 200;
    
//    LogManager.getLogger(getClass().getPackage().getName()).setLevel(Level.INFO);
    
    assertEquals(0, queryResultSetSize("*:*"));      
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
//        path + "/testBMPfp.txt",
//        path + "/boilerplate.html",
//        path + "/NullHeader.docx",
//        path + "/testWORD_various.doc",          
//        path + "/testPDF.pdf",
//        path + "/testJPEG_EXIF.jpg",
//        path + "/testXML.xml",          
//        path + "/cars.csv",
//        path + "/cars.csv.gz",
//        path + "/cars.tar.gz",
//        path + "/sample-statuses-20120906-141433.avro",
        path + "/sample-statuses-20120906-141433-medium.avro",
    };
    
    List<Event> events = new ArrayList();
    for (String file : files) {
      File f = new File(file);
      byte[] body = Files.toByteArray(f);
      Event event = EventBuilder.withBody(body);
//      event.getHeaders().put(Metadata.RESOURCE_NAME_KEY, f.getName());
      events.add(event);
    }
    
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < iters; i++) {
      if (i % 10000 == 0) {
        LOGGER.info("iter: {}", i);
      }
      for (Event event : events) {
        event = EventBuilder.withBody(event.getBody(), new HashMap(event.getHeaders()));
        event.getHeaders().put("id", UUID.randomUUID().toString());
        load(event);
      }
    }
    
    float secs = (System.currentTimeMillis() - startTime) / 1000.0f;
    long numDocs = queryResultSetSize("*:*");
    LOGGER.info("Took secs: " + secs + ", iters/sec: " + (iters/secs));
    LOGGER.info("Took secs: " + secs + ", docs/sec: " + (numDocs/secs));
    LOGGER.info("Iterations: " + iters + ", numDocs: " + numDocs);
    LOGGER.info("sink: ", sink);
  }

  private void load(Event event) throws EventDeliveryException {
    source.load(event);
  }

  private void commit() throws SolrServerException, IOException {
    solrServer.commit(false, true, true);
  }
  
  private int queryResultSetSize(String query) throws SolrServerException, IOException {
    commit();
    QueryResponse rsp = query(query);
    LOGGER.debug("rsp: {}", rsp);
    int size = rsp.getResults().size();
    return size;
  }
  
  private QueryResponse query(String query) throws SolrServerException, IOException {
    commit();
    QueryResponse rsp = solrServer.query(new SolrQuery(query).setRows(Integer.MAX_VALUE));
    LOGGER.debug("rsp: {}", rsp);
    return rsp;
  }
  
}
