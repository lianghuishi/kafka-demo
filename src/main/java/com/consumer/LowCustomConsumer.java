package com.consumer;


import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *  1） 消费者使用低级API 的主要步骤：
 *      1	根据指定的分区从主题元数据中找到主副本
 *      2	获取分区最新的消费进度
 *      3	从主副本拉取分区的消息
 *      4	识别主副本的变化，重试
 *
 *  2）方法描述：
 *      findLeader()	客户端向种子节点发送主题元数据，将副本集加入备用节点
 *      getLastOffset()	消费者客户端发送偏移量请求，获取分区最近的偏移量
 *      run()	消费者低级AP I拉取消息的主要方法
 *      findNewLeader()	当分区的主副本节点发生故障，客户将要找出新的主副本
 */

public class LowCustomConsumer {

    public static void main (String args[]) {
        //定义相关参数
        //kafka 集群
        ArrayList<String> brokers = new ArrayList<String>();
        brokers.add("mini1");
        brokers.add("mini2");
        brokers.add("mini3");
        //端口号
        int port = 9092;
        //主题
        String topic = "first";
        //分区    获取0号分区的leader
        int partition = 0;
        //offset
        long offset = 22;

        LowCustomConsumer lowCustomConsumer = new LowCustomConsumer();
        lowCustomConsumer.getdata(brokers, port, topic, partition, offset);

    }


    //找分区leader（这里只测试一个分区的lead，所以返回值为一个，如果要返回多个分区的leader，返回列表即可）
    private BrokerEndPoint findleader(List<String> brokers, int port, String topic, int partition) {

        for (String broker:brokers){
            //创建获取分区leader的消费者对象
            SimpleConsumer getLeader = new SimpleConsumer(broker, port,
                    1000, 1024*4, "getLeader");
            //创建一个主题元数据信息请求
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));

            //获取主题元数据返回值
            TopicMetadataResponse metadataResponse = getLeader.send(topicMetadataRequest);

            //解析元数据返回值
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();

            //便利主题元数据 （测试只测试一个topic，所以循环一遍就可以）
            for (TopicMetadata topicMeta:topicsMetadata){
                //获取多个元数据分区的信息
                List<PartitionMetadata> partitionsMetadata = topicMeta.partitionsMetadata();
                //遍历分区元数据
                for (PartitionMetadata partitionMetadata:partitionsMetadata){
                    if(partition==partitionMetadata.partitionId()){
                        //List<BrokerEndPoint> replicas = partitionMetadata.replicas();
                        return partitionMetadata.leader();
                    }
                }
            }

        }
        return null;
    }

    //获取数据
    private void getdata(List<String> brokers, int port, String topic, int partition,long offset){

        //获取分区leader
        BrokerEndPoint leader = findleader(brokers, port, topic, partition);
        if(leader==null){
            return ;
        }
        //获取数据的消费者对象
        SimpleConsumer getData = new SimpleConsumer(leader.host(), port,
                1000, 1024*4, "getData");
        //创建获取数据的对象(可以 .addFetch() 多个topic和partition的内容，所以fetchResponse拉取数据的时候要指定具体拉取哪一个)
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1000).build();

        //获取对象返回值
        FetchResponse fetchResponse = getData.fetch(fetchRequest);
        //解析返回值
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        //遍历打印
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            //如果要记录消费的偏移量，可以在循环结束时记录最后一个offset保存，下次拉取数据的时候就可以根据这个偏移量继续上次消费到的位置拉取数据
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println("offset1:"+offset1+"-- value:" + new String(bytes));
        }

    }

}