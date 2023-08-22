package com.shuofxz.c27_redis_demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import javax.security.auth.login.Configuration;

public class SelfRedisSink extends RichSinkFunction {
    private transient Jedis jedis;
    public void open(Configuration config) {
        jedis = new Jedis("localhost", 6379);
    }

    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        if (!jedis.isConnected()) {
            jedis.connect();
        }
        jedis.set(value.f0, value.f1);
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }
}
