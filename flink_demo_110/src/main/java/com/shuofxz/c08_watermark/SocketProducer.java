package com.shuofxz.c08_watermark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
 * @author wangshuo
 * @date 2023/8/22 10:49
 */
public class SocketProducer {
    public static void main(String[] args) {
        try {
            System.out.println("启动 socket 发送 ....");
            Socket s = new Socket("localhost", 4459);
            System.out.println("go...");
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
            String response;

            // 每隔一段时间发送一次消息
            Random r = new Random();
            String[] lang = {"flink", "spark", "hadoop", "hive", "hbase"};

            while (true) {
                Thread.sleep(1000);
                response = lang[r.nextInt(lang.length)] + "," + System.currentTimeMillis() + "\n";
                System.out.println(response);
                try {
                    bw.write(response);
                    bw.flush();
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
