package com.msopentech.xixian.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by v-wajie on 2015/11/26.
 */
public abstract class BaseTimedRichBolt extends BaseRichBolt {

    protected long interval;

    public BaseTimedRichBolt() {
        this.interval = 5;
    }

    public BaseTimedRichBolt(long interval) {
        this.interval = interval;
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        //Every interval seconds send a tick tuple
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, interval);
        return config;
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
