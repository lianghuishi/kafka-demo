package com.pruducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class NewProducer {

    public static void main(String[] args) {

//      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG

      Properties props = new Properties();
      // Kafka服务端的主机名和端口号
      props.put("bootstrap.servers", "mini1:9092");
      // 等待所有副本节点的应答
      props.put("acks", "all");
      // 消息发送最大尝试次数
      props.put("retries", 0);
      // 一批消息处理大小
      props.put("batch.size", 16384);
      // 增加服务端请求延时
      props.put("linger.ms", 1);
      // 发送缓存区内存大小
      props.put("buffer.memory", 33554432);
      // key序列化
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");


      // value序列化
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 100; i < 110; i++) {
            producer.send(new ProducerRecord<String, String>("first33", Integer.toString(i), "hello world-" + i));
        }

        producer.close();

    }
}
