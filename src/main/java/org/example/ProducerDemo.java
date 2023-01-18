package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Simple Producer");

        // criar conta no https://www.conduktor.io/ ---- gerar o topico e preencher as chaves
        String kafkakey ="";
        String kafkasecret ="";

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
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        criar o producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

//        produzir a mensagem
        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<>("java_demo","hello_word");

//      enviar a mensagem produzida
        producer.send(producerRecord);

//        limpar e fechar o producer
        producer.flush();
        producer.close();
    }
}