package com.msopentech.xixian.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据token获取设备Id
 * Created by v-wajie on 1/5/2016.
 */
public class IdTransformBolt extends AbstractJdbcBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(IdTransformBolt.class);
    private static final String ID_QUERY_SQL = "SELECT id FROM devices WHERE device_id = ? ";
    private static final String COLUMN_DEVICEID = "device_id";
    private static final String ID = "id";
    private static final String MESSAGE = "message";
    private static final String DEVICE_ASSIGNMENT_TOKEN = "deviceAssignmentToken";
    private Map<String, Integer> idMaps;

    public IdTransformBolt(ConnectionProvider connectionProvider) {
        super(connectionProvider);
        idMaps = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String msg = tuple.getStringByField(MESSAGE);
        JSONObject jsonMsg = new JSONObject(msg);

        String id = jsonMsg.getString(DEVICE_ASSIGNMENT_TOKEN);
        if (idMaps.containsKey(id)) {
            jsonMsg.put(DEVICE_ASSIGNMENT_TOKEN, idMaps.get(id));
            collector.emit(new Values(jsonMsg.toString()));
        } else {
            Integer transformedId = null;
            List<Column> columns = new ArrayList<>();
            columns.add(new Column<>(COLUMN_DEVICEID, id, Types.VARCHAR));
            List<List<Column>> resultSet = jdbcClient.select(ID_QUERY_SQL, columns);
            //结果集只有一列id。
            if (resultSet != null && !resultSet.isEmpty()) {
                List<Column> result = resultSet.get(0);
                for (Column column : result) {
                    if (column.getColumnName().equals(ID)) {
                        transformedId = (Integer) column.getVal();
                        break;
                    }
                }
            }

            if (transformedId != null) {
                //加入到idMaps中
                idMaps.put(id, transformedId);
                jsonMsg.put(DEVICE_ASSIGNMENT_TOKEN, transformedId);
                collector.emit(new Values(jsonMsg.toString()));
            } else {
                LOGGER.warn("Device(AssignmentToken = {} ) NOT Configured!", id);
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(MESSAGE));
    }
}
