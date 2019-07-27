package com.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * 高级消费者，不能直接管理消费的offset
 */
public class HightCustomConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "mini1:9092");
        // 消费组ID 消费组ID 消费组ID 消费组ID 消费组ID 消费组ID 消费组ID 消费组ID 制定consumer group
        props.put("group.id", "test3345");
        //重复消费（包括消费过的），默认是最新的，需要更换组id，以及设置earliest，代表消费最早期的
        //如果只是换组的id，那么默认是消费的是最新的，所以重复消费不了。
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 是否自动提交offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔 （把数据拿出来之后延迟提交offset）
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //如果不换组重复消费：

        //consumer.assign(Collections.singletonList(new TopicPartition("first",0 )));
        //consumer.seek(new TopicPartition("first",0 ),100);

        // 消费者订阅的topic, 可同时订阅多个
        //Collections.singletonList("first");
        consumer.subscribe(Arrays.asList("first"));
        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

}
