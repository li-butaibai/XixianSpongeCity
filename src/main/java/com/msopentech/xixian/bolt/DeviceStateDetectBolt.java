package com.msopentech.xixian.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.msopentech.xixian.Tag;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by v-wajie on 12/30/2015.
 */
public class DeviceStateDetectBolt extends BaseTimedRichBolt {

    public static final String ALERT_INSERT_STREAM = "AlertInsertStream";
    public static final String ALERT_UPDATE_STREAM = "AlertUpdateStream";
    public static final String DEVICELOG_STREAM = "DeviceLogStream";
    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceStateDetectBolt.class);
    private static final String COMMENT_OFFLINE_FORMAT = "设备(id = %s)在 %s 时间下线";
    private static final String COMMENT_ONLINE_FORMAT = "设备(id = %s)在 %s 时间上线";
    private static final String TITLE_ONLINE = "设备上线";
    private static final String TITLE_OFFLINE = "设备下线";
    private static final String TITLE_ALERT = "设备状态告警";
    private static final String DEFAULT_LEVEL = "Normal";
    public static final int DEFAULT_DIED_THRESHOLD = 5;
    private OutputCollector collector;
    private int diedThreshold;
    private Map<String, Integer> deviceMissCounters;

    private enum State {
        ACTIVE("active"), DISACTIVE("disactive"), UNKNOWN("unknown");
        private final String str;
        private State(final String str) {
            this.str = str;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    public DeviceStateDetectBolt(long detectInterval) {
        this(detectInterval, DEFAULT_DIED_THRESHOLD);
    }

    public DeviceStateDetectBolt(long detectInterval, int diedThreshold) {
        this.interval = detectInterval;
        this.diedThreshold = diedThreshold;
        deviceMissCounters = new HashMap<String, Integer>();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (isTickTuple(tuple)) {
            for (String id : deviceMissCounters.keySet()) {
                Integer counter = deviceMissCounters.get(id);
                if (counter == diedThreshold) {
                    emitDeviceStateLog(id, State.DISACTIVE);
                    emitDisactiveAlert(id);
                    LOGGER.debug("Time: {}, The Device(id = {}) disactive.", new DateTime().toLocalDateTime().toString(), id);
                }
                deviceMissCounters.put(id, counter + 1);
            }

        } else {
            String id = tuple.getStringByField("id");
            if (!deviceMissCounters.containsKey(id)) {
                emitDeviceStateLog(id, State.ACTIVE);
                LOGGER.debug("Time: {}, A new Device(id = {}) detected.",
                        new DateTime().toLocalDateTime().toString(), id);
            } else if (deviceMissCounters.get(id) > diedThreshold) {
                emitDeviceStateLog(id, State.ACTIVE);
                emitActiveAlert(id);
                LOGGER.debug("Time: {}, The Device(id = {}) restarted.",
                        new DateTime().toLocalDateTime().toString(), id);
            }

            deviceMissCounters.put(id, 0);
        }
        collector.ack(tuple);
    }

    private void emitDeviceStateLog(String id, State state) {
        String time =  new DateTime().toLocalDateTime().toString();
        String comment, title;
        if (state == State.ACTIVE) {
            title = TITLE_ONLINE;
            comment = String.format(COMMENT_ONLINE_FORMAT, id, time);
        } else {
            title = TITLE_OFFLINE;
            comment = String.format(COMMENT_OFFLINE_FORMAT, id, time);
        }

        Values values = new Values(Tag.DeviceLog, id, new Date().getTime(), title, comment);
        collector.emit(DEVICELOG_STREAM, values);
    }

    private void emitDisactiveAlert(String id) {
        String time =  new DateTime().toLocalDateTime().toString();
        Values values = new Values(Tag.DisactiveAlert, id, TITLE_ALERT,
                String.format(COMMENT_OFFLINE_FORMAT, id, time), new Date().getTime(),
                0, State.DISACTIVE, DEFAULT_LEVEL);
        collector.emit(ALERT_INSERT_STREAM, values);
    }

    private void emitActiveAlert(String id) {
        String time =  new DateTime().toLocalDateTime().toString();
        Values values = new Values(Tag.ActiveAlert, id, TITLE_ALERT,
                String.format(COMMENT_OFFLINE_FORMAT, id, time), 0,
                new Date().getTime(), State.ACTIVE, DEFAULT_LEVEL);
        collector.emit(ALERT_UPDATE_STREAM, values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(ALERT_INSERT_STREAM, new Fields("tag", "deviceid",
                "title", "comments", "createtime", "endtime", "state", "level"));
        outputFieldsDeclarer.declareStream(ALERT_UPDATE_STREAM, new Fields("tag", "deviceid",
                "title", "comments", "createtime", "endtime", "state", "level"));
        outputFieldsDeclarer.declareStream(DEVICELOG_STREAM, new Fields("tag", "deviceid", "logtime",
                "logtitle", "comments"));
    }
}
