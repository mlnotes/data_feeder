package com.mlnotes.storm.feeder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Zhu Hanfeng <me@mlnotes.com>
 */
public class App {
    private final Jedis jedis = new Jedis(Constants.REDIS_SERVER);
    
    // TODO how to control feeding rate
    public void pushData() throws IOException{
        InputStream is = this.getClass().getResourceAsStream(Constants.DATA);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String line;
        int count = 0;
        while((line = reader.readLine()) != null && count < 100){
            jedis.rpush(Constants.QUEUE_NAME, line);
            count++;
        }
    }
    
    public static void main(String[] args) throws IOException{
        App app = new App();
        app.pushData();
    }
}
