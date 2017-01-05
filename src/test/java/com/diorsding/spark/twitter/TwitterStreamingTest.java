package com.diorsding.spark.twitter;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * Created by jiashan on 12/29/16.
 */
public class TwitterStreamingTest {

    @Test
    public void test() {
        Jedis jedis = new Jedis(Constants.REDIS_CONNECTION_HOST, Constants.REDIS_CONNECTION_PORT);
        Pipeline pipelined = jedis.pipelined();

        for (int i =0 ; i < 100; i++) {
            pipelined.publish("test", "houhouhou " + i);
            System.out.println("houhouhou " + i);
        }
        pipelined.sync();
    }

    @Test
    public void test2() {
        Jedis jedis = new Jedis(Constants.REDIS_CONNECTION_HOST, Constants.REDIS_CONNECTION_PORT);

        jedis.set("foo", "bar");

        System.out.print(jedis.get("foo"));
    }
}
