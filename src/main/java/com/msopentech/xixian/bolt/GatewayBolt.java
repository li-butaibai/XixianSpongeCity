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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by v-wajie on 12/30/2015.
 */
public class GatewayBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(GatewayBolt.class);
    public static final String DEVICE_ID_STREAM = "DeviceStream";
    public static final String MEASUREMENTS_STREAM = "MeasurementsStream";
    private static final String TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private SimpleDateFormat dateFormat = new SimpleDateFormat(TIME_PATTERN);
    private static final String MSG = "message";
    private static final String DEVICE_ID = "deviceAssignmentToken";
    private static final String EVENT_TYPE = "eventType";
    private static final String MEASUREMENTS_TYPE = "Measurements";
    private static final String EVENT_DATE = "eventDate";
    private static final String MEASUREMENTS = "measurements";
    private static final String RAIN = "rain";
    private static final String TEMPERATURE = "airtemp";
    private static final String WIND_SPEED = "windspeed";
    private static final String SOLAR_RADIATION = "solarradiation";
    private static final String SOIL_HUMID = "soilhumid";

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String msg = tuple.getStringByField(MSG);
        LOGGER.debug("Received Msg: {}", msg);
        JSONObject jsonMsg = new JSONObject(msg);

        String id = jsonMsg.getString(DEVICE_ID);
        collector.emit(DEVICE_ID_STREAM, new Values(id));

        //If the Msg contains measurements then parse measurements and send.
        if (jsonMsg.has(EVENT_TYPE) && jsonMsg.getString(EVENT_TYPE).equals(MEASUREMENTS_TYPE)) {
            JSONObject measurements = (JSONObject) jsonMsg.get(MEASUREMENTS);
            //The default time is now
            Date eventDate = new Date();
            try {
                eventDate = dateFormat.parse(jsonMsg.getString(EVENT_DATE));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if (measurements.has(RAIN)) {
                collector.emit(MEASUREMENTS_STREAM, new Values(Tag.Measurements, id,
                        eventDate.getTime(), DateType.RainFall.getType(), measurements.getDouble(RAIN)));
            }
            if (measurements.has(TEMPERATURE)) {
                collector.emit(MEASUREMENTS_STREAM, new Values(Tag.Measurements, id,
                        eventDate.getTime(), DateType.Temperature.getType(), measurements.getDouble(TEMPERATURE)));
            }
            if (measurements.has(WIND_SPEED)) {
                collector.emit(MEASUREMENTS_STREAM, new Values(Tag.Measurements, id,
                        eventDate.getTime(), DateType.WindSpeed.getType(), measurements.getDouble(WIND_SPEED)));
            }
            if (measurements.has(SOLAR_RADIATION)) {
                collector.emit(MEASUREMENTS_STREAM, new Values(Tag.Measurements, id,
                        eventDate.getTime(), DateType.Radiation.getType(), measurements.getDouble(SOLAR_RADIATION)));
            }
            if (measurements.has(SOIL_HUMID)) {
                collector.emit(MEASUREMENTS_STREAM, new Values(Tag.Measurements, id,
                        eventDate.getTime(), DateType.SoilMoisture.getType(), measurements.getDouble(SOIL_HUMID)));
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
