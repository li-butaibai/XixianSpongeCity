package com.msopentech.xixian;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by v-wajie on 2015/11/24.
 */
public class PropertyUtil {
    private final static Logger logger =
            LoggerFactory.getLogger(PropertyUtil.class);


    public static Properties loadProperties(String propertiesFileName) {
        Properties properties = new Properties();
        try {
            logger.info("Loading Properties File: {}", propertiesFileName);
            InputStream inputStream = PropertyUtil.class.getClassLoader().getResourceAsStream(
                    propertiesFileName + ".properties");
            properties.load(inputStream);
            logger.info("Successfully loaded Properties File: {}", propertiesFileName);
        } catch (Exception e) {
            logger.info("can't open %s.properties file!", propertiesFileName);
            e.printStackTrace();
            System.exit(-1);
        }
        return properties;
    }
}
