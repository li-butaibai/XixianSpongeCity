package com.msopentech.xixian;

/**
 * Created by v-wajie on 12/30/2015.
 */
public enum DateType {
    RainFall(1),
    Temperature(2),
    Radiation(3),
    WindSpeed(4),
    Dewatering(5),
    InFlow(6),
    OutFlow(7),
    SoilMoisture(8),
    UndergroundWaterLevel(9),
    UdergroundWaterTemperature(10);

    private int type;
    private DateType(Integer n) {
        this.type = n;
    }
    public int getType() {
        return type;
    }
}
