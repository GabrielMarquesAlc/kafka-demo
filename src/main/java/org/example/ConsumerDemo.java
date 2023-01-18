package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        log.info("I am a Consumer");
        // criar conta no https://www.conduktor.io/ ---- gerar o topico e preencher as chaves
        String kafkakey ="";
        String kafkasecret ="";
        String groupId = "my-java-application";
        String topic = "java_demo";
//        Criar a propriedade do producer
        Properties properties = new Properties();
        // conectar ao local host
        //  properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        // conectar ao playground
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                kafkakey, kafkasecret));
        properties.setProperty("sasl.mechanism","PLAIN");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));
        while(true){
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String,String> record: records){
                log.info("Key: "+record.key() +", Value: "+ record.value());
                log.info("Partition: "+record.partition() +", Offset: "+ record.offset());
            }
        }



    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
}