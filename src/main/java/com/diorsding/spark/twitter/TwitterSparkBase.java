package com.diorsding.spark.twitter;

import com.diorsding.spark.utils.LogUtils;
import com.diorsding.spark.utils.OAuthUtils;

import org.apache.log4j.Level;
import org.json.simple.parser.ParseException;

import twitter4j.Status;

import java.io.IOException;

/**
 * Created by jiashan on 12/28/16.
 */
public class TwitterSparkBase {
    protected static void preSetup() throws IOException, ParseException {
        LogUtils.setSparkLogLevel(Level.WARN, Level.WARN);

        OAuthUtils.configureTwitterCredentials();

        // TODO: Check DB tables. If not exist, create table automatically.
        // CassandraUtils.setupCanssadraTables();
    }

    protected static boolean isTweetEnglish(Status status) {
        return "en".equals(status.getLang()) && "en".equals(status.getUser().getLang());
    }
}
