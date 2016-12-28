package com.diorsding.spark.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Created by jiashan on 12/27/16.
 */
public class LogUtils {

    public static void setSparkLogLevel(Level sparkLogLevel, Level streamingLogLevel) {
        Logger.getLogger("org.apache.spark").setLevel(sparkLogLevel);
        Logger.getLogger("org.apache.spark.streaming.NetworkInputTracker").setLevel(streamingLogLevel);
    }
}
