package com.msopentech.xixian.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.msopentech.xixian.Tag;
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by v-wajie on 2015/12/1.
 */
public class JdbcStoreBolt extends AbstractJdbcBolt{

    private Map<Tag, JdbcMapper> mappers;
    private Map<Tag, String> sqlStatements;
    private Set<Tag> registeredTags;
    private final static int DEFAULT_QUERY_TIMEOUT_SECS = 10;
    private final Logger logger = LoggerFactory.getLogger(JdbcStoreBolt.class);


    public JdbcStoreBolt(ConnectionProvider connectionProvider, int timeoutSecs) {
        super(connectionProvider);
        this.queryTimeoutSecs = timeoutSecs;
        mappers = new HashMap<Tag, JdbcMapper>();
        sqlStatements = new HashMap<Tag, String>();
        registeredTags = new TreeSet<Tag>();
    }

    public JdbcStoreBolt(ConnectionProvider connectionProvider) {
        this(connectionProvider, DEFAULT_QUERY_TIMEOUT_SECS);
    }

    public void register(Tag tag, JdbcMapper mapper, String insertSql) {
        registeredTags.add(tag);
        mappers.put(tag, mapper);
        sqlStatements.put(tag, insertSql);
    }

    public void remove(Tag tag) {
        if (isRegistered(tag)) {
            registeredTags.remove(tag);
            mappers.remove(tag);
            sqlStatements.remove(tag);
        }
    }

    public boolean isRegistered(Tag tag) {
        return registeredTags.contains(tag);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        if (registeredTags.isEmpty()) {
            throw new IllegalArgumentException("You must supply at least one stream");
        }
    }

    public void execute(Tuple tuple) {

        logger.warn("Time: {}. Received Tuple: {} and processed time is {}.", new DateTime().toLocalDateTime().toString(),
                tuple.getValues().toString(), new DateTime(tuple.getLongByField("datetime")).toLocalDateTime().toString());

        collector.ack(tuple);

        final Tuple myTuple = tuple;
        final Tag tag = (Tag) tuple.getValueByField("tag");
        if (tag != null && isRegistered(tag)) {
            List<Column> columns = mappers.get(tag).getColumns(tuple);
            final List<List<Column>> columnLists = new ArrayList<List<Column>>();
            columnLists.add(columns);
            new Thread(new Runnable() {
                public void run() {
                    jdbcClient.executeInsertQuery(sqlStatements.get(tag), columnLists);
                }
            }).start();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}

