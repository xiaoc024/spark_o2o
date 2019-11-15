package com.xiaoc024.spark.online;

import com.alibaba.fastjson.JSONObject;
import com.xiaoc024.spark.ParamsConf;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.clients.log4jappender.Log4jAppender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class LogGenerator {

    private static Logger downloadLogger = Logger.getLogger("downloadLogger");
    private static Logger consumeLogger = Logger.getLogger("consumeLogger");

    private static Random random = new Random();

    public static void main(String[] args) throws Exception{

        BasicConfigurator.resetConfiguration();

        Log4jAppender downloadLogAppender = new Log4jAppender(ParamsConf.log4jHost(),ParamsConf.log4jPort1());
        downloadLogAppender.setUnsafeMode(true);
        downloadLogAppender.activateOptions();

        Log4jAppender consumeLoggerAppender = new Log4jAppender(ParamsConf.log4jHost(),ParamsConf.log4jPort2());
        consumeLoggerAppender.setUnsafeMode(true);
        consumeLoggerAppender.activateOptions();

        downloadLogger.addAppender(downloadLogAppender);
        consumeLogger.addAppender(consumeLoggerAppender);

        while(true) {
            Thread.sleep(random.nextInt(100));
            downloadLogger.info(generateDownloadLog());
            consumeLogger.info(generateConsumeLog());
        }
    }

    /**
     *
     * {"time":"20191106093559",
     * "user_id":958,
     * "game_id":220}
     *
     */
    private static String generateDownloadLog() {
        Map<String,Object> map = new HashMap<>();
        map.put("time", FastDateFormat.getInstance("yyyyMMddHHmmss").format(new Date()));
        map.put("user_id",random.nextInt(10000));
        map.put("game_id",random.nextInt(1000000));
        JSONObject jsonObject = new JSONObject(map);
        return jsonObject.toJSONString();
    }

    /**
     *
     *{"flag":"1",
     * "order_id":"2b7ce0c4-9d4e-4ac9-8c28-62fdda71a032",
     * "fee":193,
     * "time":"20191106093559",
     * "user_id":958,
     * "game_id":220}
     *
     */
    private static String generateConsumeLog() {
        Map<String,Object> map = new HashMap<>();
        String[] result = {"0","1"}; // 0未成功支付，1成功支付
        map.put("flag",result[random.nextInt(2)]);
        map.put("order_id", UUID.randomUUID().toString());
        map.put("fee",random.nextInt(300));
        map.put("time", FastDateFormat.getInstance("yyyyMMddHHmmss").format(new Date()));
        map.put("user_id",random.nextInt(10000));
        map.put("game_id",random.nextInt(1000000));
        JSONObject jsonObject = new JSONObject(map);
        return jsonObject.toJSONString();
    }
}
