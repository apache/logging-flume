package org.apache.flume.sink.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by roger.lu on 17/4/24.
 */
public class CassandraSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(CassandraSink.class);

  public static final String CONFIG_SERIALIZER = "serializer";

  public static final String CASSANDRA_CONTACT_POINTS = "cassandra.contactPoints";
  public static final String CASSANDRA_USERNAME = "cassandra.username";
  public static final String CASSANDRA_PASSWORD = "cassandra.password";
  public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
  public static final String CASSANDRA_TABLE = "cassandra.table";
  public static final String CONFIG_TIME_FORMAT = "datetime.format";
  public static final String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ssZ";

  private CassandraEventSerializer serializer;
  private Cluster cluster;
  private Session session;
  private String keyspace;
  private String datetimeFormat = DEFAULT_TIME_FORMAT;
  private String table = null;

  private List<String> allColumns = Lists.newArrayList();
  private List<String> primaryColumns = Lists.newArrayList();
  private List<String> collectionColumns = Lists.newArrayList();

  private CodecRegistry codecRegistry;
  private TableMetadata tableMetadata;

  @Override
  public void configure(Context context) {

    try {
      String serializerType = context.getString(CONFIG_SERIALIZER);
      Class<? extends CassandraEventSerializer> serializerClass
        = (Class<? extends CassandraEventSerializer>) Class.forName(serializerType);
      serializer = serializerClass.newInstance();
      serializer.configure(context);

      keyspace = context.getString(CASSANDRA_KEYSPACE);
      table = context.getString(CASSANDRA_TABLE);
      cluster = Cluster.builder()
        .addContactPoints(context.getString(CASSANDRA_CONTACT_POINTS).split(","))
        .withCredentials(context.getString(CASSANDRA_USERNAME), context.getString(CASSANDRA_PASSWORD))
        .build();
      session = cluster.connect(keyspace);
      codecRegistry = cluster.getConfiguration().getCodecRegistry();
      tableMetadata = cluster.getMetadata().getKeyspace(keyspace).getTable(table);

      //get primary columns
      List<ColumnMetadata> primaryKey = tableMetadata.getPrimaryKey();
      for (ColumnMetadata pk : primaryKey) {
        primaryColumns.add(pk.getName());
      }

      //get collection columns
      List<ColumnMetadata> columns = tableMetadata.getColumns();
      for (ColumnMetadata cm : columns) {
        allColumns.add(cm.getName());
        if (cm.getType().isCollection()) {
          collectionColumns.add(cm.getName());
        }
      }

      //customized datetime format
      if (!Strings.isNullOrEmpty(context.getString(CONFIG_TIME_FORMAT))) {
        datetimeFormat = context.getString(CONFIG_TIME_FORMAT);
      }

    } catch (Exception e) {
      logger.error("Could not instantiate event serializer.", e);
      Throwables.propagate(e);
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public Status process() throws EventDeliveryException {

    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    txn.begin();

    try {
      Event event = channel.take();
      if (null != event) {
        String body = new String(event.getBody(), Charsets.UTF_8);
        if (!Strings.isNullOrEmpty(body)) {
          logger.info("start to sink event [{}].", body);
          Map<String, Object> eventMap = serializer.getActions(event.getBody());

          //primary columns verify
          for (String cl : primaryColumns) {
            if (!eventMap.containsKey(cl)) {
              logger.error("primary key {} not existed.", cl);
              return Status.BACKOFF;
            }
          }

          for (Map.Entry<String, Object> entry : eventMap.entrySet()) {
            //transform datetime
            if (null != tableMetadata.getColumn(entry.getKey())) {
              DataType dataType = tableMetadata.getColumn(entry.getKey()).getType();
              TypeCodec<Object> typeCodec = codecRegistry.codecFor(dataType);
              if (typeCodec.accepts(Date.class)) {
                SimpleDateFormat parsedFormat = new SimpleDateFormat(datetimeFormat, Locale.US);
                Date parsed = parsedFormat.parse(entry.getValue().toString());
                entry.setValue(parsed);
              }
            }
          }

          Update buildStatement = getInsertOrUpdateStatement(eventMap);
          logger.info("to update clause: {}.", buildStatement.toString());
          ResultSet rs = session.execute(buildStatement);

          if (!rs.wasApplied()) {
            logger.error("fail to insert event [{}].", body);
          }
          logger.info("sink event [{}] successfully.", body);
        }
      }
      txn.commit();
      return Status.READY;
    } catch (Throwable tx) {
      try {
        txn.rollback();
      } catch (Exception ex) {
        logger.error("exception in rollback.", ex);
      }
      logger.error("transaction rolled back.", tx);
      return Status.BACKOFF;
    } finally {
      txn.close();
    }

  }

  public Update getInsertOrUpdateStatement(Map<String, Object> eventMap) {

    Update update = QueryBuilder.update(keyspace, table);

    for (Map.Entry<String, Object> entry : eventMap.entrySet()) {
      //ignore event key not in columns
      if (!allColumns.contains(entry.getKey())) {
        continue;
      }

      if (primaryColumns.contains(entry.getKey())) {
        update.where(QueryBuilder.eq(entry.getKey(), entry.getValue()));
      } else if (collectionColumns.contains(entry.getKey())) {
        update.with(QueryBuilder.add(entry.getKey(), entry.getValue()));
      } else {
        update.with(QueryBuilder.set(entry.getKey(), entry.getValue()));
      }
    }
    return update;
  }

  public Session getSession() {
    return session;
  }

  public void setSession(Session session) {
    this.session = session;
  }
}
