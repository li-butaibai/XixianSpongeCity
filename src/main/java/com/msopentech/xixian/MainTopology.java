package com.msopentech.xixian;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.microsoft.eventhubs.spout.EventHubSpout;
import com.microsoft.eventhubs.spout.EventHubSpoutConfig;
import com.msopentech.xixian.bolt.DeviceStateDetectBolt;
import com.msopentech.xixian.bolt.GatewayBolt;
import com.msopentech.xixian.bolt.IdTransformBolt;
import com.msopentech.xixian.bolt.JdbcStoreBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.shade.org.joda.time.DateTime;

import java.sql.Types;
import java.util.*;

/**
 * 在MainTopology中配置Storm的各个节点。
 * Created by v-wajie on 12/30/2015.
 */
public class MainTopology {

    private static final String TOPOLOGY_NAME = "Xixian";
    private boolean runLocal = true;
    private int numWorkers;
    private static final Long HEART_BEAT_INTERVAL_IN_SECS = 5L;

    //数据库语句
    private static final String DATA_INSERT_SQL = "INSERT INTO data(device_id, datatime, datatype_id, datavalue) " +
            "VALUES (?, ?, ?, ?)";
    private static final String ALERT_INSERT_SQL = "INSERT INTO alert(device_id, title, comments, " +
            "createtime, state, level)" + "VALUES (?, ?, ?, ?, ?, ?)";
    private static final String LOG_INSERT_SQL = "INSERT INTO devicelog(device_id, logtime, logtitle, comments)" +
            "VALUES (?, ?, ?, ?)";
    private static final String ALERT_UPDATE_SQL = "UPDATE alert SET endtime = ? WHERE id = (SELECT max(id) " +
            " FROM alert WHERE device_id = ? )";
    private static final String UPDATE_STATE_SQL = "UPDATE devices SET state = ? WHERE device_id = ? ";

    //数据库连接
    private ConnectionProvider connectionProvider;

    public ConnectionProvider getConnectionProvider() {
        if (connectionProvider == null) {
            connectionProvider = new HikariCPConnectionProvider(getAzureSQLConfig());
        }
        return connectionProvider;
    }

    /**
     * 初始化EventHub配置
     * @param enableTimeFilter 是否启动过滤器（只接受当前时间开始的信息）
     * */
    private EventHubSpoutConfig initEventHubConfig(Boolean enableTimeFilter) {
        Properties eventHubProps = PropertyUtil.loadProperties("eventhub");
        String username = eventHubProps.getProperty("username");
        String password = eventHubProps.getProperty("password");
        String namespaceName = eventHubProps.getProperty("namespaceName");
        String entityPath = eventHubProps.getProperty("entityPath");
        String targetAddress = eventHubProps.getProperty("targetAddress");
        int partitionCount = Integer.parseInt(eventHubProps.getProperty("partitionCount"));

        EventHubSpoutConfig config = new EventHubSpoutConfig(username, password, namespaceName,
                entityPath, partitionCount);
        config.setTargetAddress(targetAddress);
        if (enableTimeFilter)
            config.setEnqueueTimeFilter(new DateTime().getMillis());
        numWorkers = config.getPartitionCount();
        return config;
    }

    /**
     * 从配置文件sqlserver.properties中加载SQL Server配置
     * */
    private Map<String, Object> getAzureSQLConfig() {
        Properties sqlServerProps = PropertyUtil.loadProperties("sqlserver");
        Map<String, Object> map = new HashMap<String, Object>();
        for (Object key : sqlServerProps.keySet()) {
            map.put(key.toString(), sqlServerProps.get(key));
        }
        return map;
    }

    /**
     * 创建存储到数据库的Bolt节点
     * */
    private JdbcStoreBolt buildJdbcStoreBolt() {
        JdbcStoreBolt jdbcStoreBolt = new JdbcStoreBolt(getConnectionProvider());

        List<Column> schemaColumns = new ArrayList<Column>();
        schemaColumns.add(new Column("device_id", Types.INTEGER));
        schemaColumns.add(new Column("datatime", Types.TIMESTAMP));
        schemaColumns.add(new Column("datatype_id", Types.INTEGER));
        schemaColumns.add(new Column("datavalue", Types.FLOAT));
        SimpleJdbcMapper dataMapper = new SimpleJdbcMapper(schemaColumns);
        jdbcStoreBolt.register(Tag.Measurements, dataMapper, DATA_INSERT_SQL);


        schemaColumns = new ArrayList<Column>();
        schemaColumns.add(new Column("device_id", Types.INTEGER));
        schemaColumns.add(new Column("title", Types.VARCHAR));
        schemaColumns.add(new Column("comments", Types.VARCHAR));
        schemaColumns.add(new Column("createtime", Types.TIMESTAMP));
        schemaColumns.add(new Column("state", Types.VARCHAR));
        schemaColumns.add(new Column("level", Types.INTEGER));
        SimpleJdbcMapper alertInsertMapper = new SimpleJdbcMapper(schemaColumns);
        jdbcStoreBolt.register(Tag.DisactiveAlert, alertInsertMapper, ALERT_INSERT_SQL);

        schemaColumns = new ArrayList<Column>();
        schemaColumns.add(new Column("device_id", Types.INTEGER));
        schemaColumns.add(new Column("logtime", Types.TIMESTAMP));
        schemaColumns.add(new Column("logtitle", Types.VARCHAR));
        schemaColumns.add(new Column("comments", Types.VARCHAR));
        SimpleJdbcMapper logMapper = new SimpleJdbcMapper(schemaColumns);
        jdbcStoreBolt.register(Tag.DeviceLog, logMapper, LOG_INSERT_SQL);

        schemaColumns = new ArrayList<Column>();
        schemaColumns.add(new Column("endtime", Types.TIMESTAMP));
        schemaColumns.add(new Column("device_id", Types.INTEGER));
        SimpleJdbcMapper alertUpdateMapper = new SimpleJdbcMapper(schemaColumns);
        jdbcStoreBolt.register(Tag.ActiveAlert, alertUpdateMapper, ALERT_UPDATE_SQL);

        schemaColumns = new ArrayList<>();
        schemaColumns.add(new Column("state", Types.VARCHAR));
        schemaColumns.add(new Column("device_id", Types.INTEGER));
        SimpleJdbcMapper updateStateMapper = new SimpleJdbcMapper(schemaColumns);
        jdbcStoreBolt.register(Tag.DeviceState, updateStateMapper, UPDATE_STATE_SQL);

        return jdbcStoreBolt;
    }

    /**
     * 创建Storm Topology
     * */
    private StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        //first to init EventHubSpoutConfig
        //enable Time filter
        EventHubSpoutConfig config = initEventHubConfig(true);
        EventHubSpout eventHubSpout = new EventHubSpout(config);

        JdbcStoreBolt jdbcStoreBolt = buildJdbcStoreBolt();
        DeviceStateDetectBolt stateDetectBolt = new DeviceStateDetectBolt(HEART_BEAT_INTERVAL_IN_SECS);

        builder.setSpout("EventHubSpout", eventHubSpout, numWorkers);
        builder.setBolt("IdTransformBolt", new IdTransformBolt(getConnectionProvider()), numWorkers)
                .localOrShuffleGrouping("EventHubSpout");
        builder.setBolt("GatewayBolt", new GatewayBolt(), numWorkers)
                .localOrShuffleGrouping("IdTransformBolt");
        builder.setBolt("DeviceStateDetectBolt",stateDetectBolt, 2)
                .fieldsGrouping("GatewayBolt", GatewayBolt.DEVICE_ID_STREAM, new Fields("id"));
        builder.setBolt("JdbcStoreBolt", jdbcStoreBolt, 1)
                .localOrShuffleGrouping("GatewayBolt", GatewayBolt.MEASUREMENTS_STREAM)
                .localOrShuffleGrouping("DeviceStateDetectBolt", DeviceStateDetectBolt.ALERT_INSERT_STREAM)
                .localOrShuffleGrouping("DeviceStateDetectBolt", DeviceStateDetectBolt.DEVICELOG_STREAM)
                .localOrShuffleGrouping("DeviceStateDetectBolt", DeviceStateDetectBolt.ALERT_UPDATE_STREAM)
                .localOrShuffleGrouping("DeviceStateDetectBolt", DeviceStateDetectBolt.STATE_UPDATE_STREAM);

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        MainTopology mainTopology = new MainTopology();
        mainTopology.runScenario(args);

    }

    private void runScenario(String[] args) throws Exception {
        StormTopology topology = buildTopology();
        Config config = new Config();
        if (runLocal) {
            config.setMaxTaskParallelism(numWorkers);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(TOPOLOGY_NAME, config, topology);
        } else {
            config.setNumWorkers(numWorkers);
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, topology);
        }
    }


}
