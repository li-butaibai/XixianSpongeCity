package com.msopentech.xixian.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.msopentech.xixian.Tag;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;

import java.util.List;

/**
 * Created by v-wajie on 12/30/2015.
 */
public class JdbcUpdateBolt extends JdbcStoreBolt {

    public JdbcUpdateBolt(ConnectionProvider connectionProvider, int timeoutSecs) {
        super(connectionProvider, timeoutSecs);
    }

    public JdbcUpdateBolt(ConnectionProvider connectionProvider) {
        super(connectionProvider);
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);

        final Tag tag = (Tag) tuple.getValueByField("tag");
        if (tag != null && isRegistered(tag)) {
            List<Column> columns = mappers.get(tag).getColumns(tuple);
            final String sqlStatement = generateSQLStatement(tag, columns);
            new Thread(new Runnable() {
                @Override
                public void run() {
                    jdbcClient.executeSql(sqlStatement);
                }
            }).start();
        }
    }

    private String generateSQLStatement(Tag tag, List<Column> columns) {
        String sqlStatement = sqlStatements.get(tag);
        sqlStatement.replace("?", "%s");
        return String.format(sqlStatement, columns);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
