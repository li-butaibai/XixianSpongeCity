package com.msopentech.xixian;

/**
 * Created by v-wajie on 12/30/2015.
 */
public enum DateType {
    RainFall(0),
    Temperature(1),
    Radiation(2),
    WindSpeed(3),
    Dewatering(4),
    InflowRate(5),
    OutflowRate(6),
    SoilMoisture(7),
    UndergroundWaterLevel(8),
    UdergroundWaterTemperature(9);

    private int type;
    private DateType(Integer n) {
        this.type = n;
    }
    public int getType() {
        return type;
    }
}
