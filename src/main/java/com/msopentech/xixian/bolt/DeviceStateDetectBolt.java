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
 * 用于检测Device的状态
 * Created by v-wajie on 12/30/2015.
 */
public class DeviceStateDetectBolt extends BaseTimedRichBolt {

    public static final String ALERT_INSERT_STREAM = "AlertInsertStream";
    public static final String ALERT_UPDATE_STREAM = "AlertUpdateStream";
    public static final String DEVICELOG_STREAM = "DeviceLogStream";
    public static final String STATE_UPDATE_STREAM = "StateUpdateStream";
    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceStateDetectBolt.class);
    private static final String COMMENT_OFFLINE_FORMAT = "设备(id = %s)在 %s 时间下线";
    private static final String COMMENT_ONLINE_FORMAT = "设备(id = %s)在 %s 时间上线";
    private static final String TITLE_ONLINE = "设备上线";
    private static final String TITLE_OFFLINE = "设备下线";
    private static final String TITLE_ALERT = "设备状态告警";
    private static final int DEFAULT_LEVEL = 1;
    public static final int DEFAULT_DIED_THRESHOLD = 5;
    private OutputCollector collector;
    private int diedThreshold;
    private Map<Integer, Integer> deviceMissCounters;

    private enum State {
        ACTIVE("active"), DISACTIVE("disactive"), UNKNOWN("unknown"), ONLINE("online"), OFFLINE("offline");
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
        deviceMissCounters = new HashMap<>();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (isTickTuple(tuple)) {
            for (Integer id : deviceMissCounters.keySet()) {
                int counter = deviceMissCounters.get(id);
                if (counter == diedThreshold) {
                    emitUpdateState(id, State.OFFLINE);
                    emitDeviceStateLog(id, State.DISACTIVE);
                    emitDisactiveAlert(id);
                    LOGGER.debug("Time: {}, The Device(id = {}) disactive.", new DateTime().toLocalDateTime().toString(), id);
                }
                deviceMissCounters.put(id, counter + 1);
            }

        } else {
            Integer id = tuple.getIntegerByField("id");
            if (!deviceMissCounters.containsKey(id)) {
                emitDeviceStateLog(id, State.ACTIVE);
                emitUpdateState(id, State.ONLINE);
                LOGGER.debug("Time: {}, A new Device(id = {}) detected.",
                        new DateTime().toLocalDateTime().toString(), id);
            } else if (deviceMissCounters.get(id) > diedThreshold) {
                emitDeviceStateLog(id, State.ACTIVE);
                emitActiveAlert(id);
                emitUpdateState(id, State.ONLINE);
                LOGGER.debug("Time: {}, The Device(id = {}) restarted.",
                        new DateTime().toLocalDateTime().toString(), id);
            }
            deviceMissCounters.put(id, 0);
        }
        collector.ack(tuple);
    }

    private void emitDeviceStateLog(Integer id, State state) {
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

    private void emitDisactiveAlert(Integer id) {
        String time =  new DateTime().toLocalDateTime().toString();
        Values values = new Values(Tag.DisactiveAlert, id, TITLE_ALERT,
                String.format(COMMENT_OFFLINE_FORMAT, id, time), new Date().getTime(),
                0, State.DISACTIVE.toString(), DEFAULT_LEVEL);
        collector.emit(ALERT_INSERT_STREAM, values);
    }

    private void emitActiveAlert(Integer id) {
        String time =  new DateTime().toLocalDateTime().toString();
        Values values = new Values(Tag.ActiveAlert, id, TITLE_ALERT,
                String.format(COMMENT_OFFLINE_FORMAT, id, time), 0,
                new Date().getTime(), State.ACTIVE.toString(), DEFAULT_LEVEL);
        collector.emit(ALERT_UPDATE_STREAM, values);
    }


    private void emitUpdateState(Integer id, State state) {
        Values values = new Values(Tag.DeviceState, id, state.toString());
        collector.emit(STATE_UPDATE_STREAM, values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(ALERT_INSERT_STREAM, new Fields("tag", "device_id",
                "title", "comments", "createtime", "endtime", "state", "level"));
        outputFieldsDeclarer.declareStream(ALERT_UPDATE_STREAM, new Fields("tag", "device_id",
                "title", "comments", "createtime", "endtime", "state", "level"));
        outputFieldsDeclarer.declareStream(DEVICELOG_STREAM, new Fields("tag", "device_id", "logtime",
                "logtitle", "comments"));
        outputFieldsDeclarer.declareStream(STATE_UPDATE_STREAM, new Fields("tag", "device_id", "state"));
    }
}
