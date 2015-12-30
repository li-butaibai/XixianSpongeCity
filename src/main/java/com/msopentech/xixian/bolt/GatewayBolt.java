package com.msopentech.xixian.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.msopentech.xixian.DateType;
import com.msopentech.xixian.Tag;
import org.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by v-wajie on 12/30/2015.
 */
public class GatewayBolt extends BaseRichBolt {

    public static final String DEVICE_ID_STREAM = "DeviceStream";
    public static final String MEASUREMENTS_STREAM = "MeasurementsStream";
    private SimpleDateFormat dateFormat = new SimpleDateFormat();
    private static final String MSG = "message";
    private static final String DEVICE_ID = "deviceAssignmentToken";
    private static final String EVENT_TYPE = "eventType";
    private static final String MEASUREMENTS_TYPE = "Measurements";
    private static final String EVENT_DATE = "eventDate";
    private static final String MEASUREMENTS = "measurements";
    private static final String RAIN = "rain";
    private static final String TEMPERATURE = "airtemp";


    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String msg = tuple.getStringByField(MSG);
        JSONObject jsonMsg = new JSONObject(msg);

        String id = jsonMsg.getString(DEVICE_ID);
        collector.emit(DEVICE_ID_STREAM, new Values(id));

        //If the Msg contains measurements then parse measurements and send.
        if (jsonMsg.has(EVENT_TYPE) && jsonMsg.get(EVENT_TYPE).equals(MEASUREMENTS_TYPE)) {
            JSONObject measurements = (JSONObject) jsonMsg.get(MEASUREMENTS);
            Date eventDate = null;
            try {
                eventDate = dateFormat.parse(jsonMsg.getString(EVENT_DATE));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if (measurements.has(RAIN)) {
                collector.emit(MEASUREMENTS_STREAM,
                        new Values(Tag.Measurements, id, eventDate, DateType.RainFall, measurements.getDouble(RAIN)));
            }
            if (measurements.has(TEMPERATURE)) {
                collector.emit(MEASUREMENTS_STREAM,
                        new Values(Tag.Measurements, id, eventDate, DateType.Temperature, measurements.getDouble(TEMPERATURE)));
            }

        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DEVICE_ID_STREAM, new Fields("id"));
        outputFieldsDeclarer.declareStream(MEASUREMENTS_STREAM, new Fields("tag", "deviceid", "datatime", "datatype_id", "datavalue"));
    }
}
