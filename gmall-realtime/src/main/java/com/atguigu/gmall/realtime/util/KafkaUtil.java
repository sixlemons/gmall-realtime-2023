package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.print.DocFlavor;
import java.util.Properties;

/**
 * @Title: KafkaUtil
 * @Author ning
 * @Package com.atguigu.gmall.realtime.util
 * @Date 2023/12/10 17:30
 * @description: kafka工具类
 */
public class KafkaUtil {
    static String BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    static String DEFAULT_TOPIC = "default_topic";
    
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            //判定是流中的最后的一个元素
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            //定义反序列化

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if (record != null && record.value() != null){
                    return new String(record.value());
                }
                return null;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, properties);
        return consumer;
    }
}
