package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Simple Producer with Keys");

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
        for (int j=0;j<10;j++){
    //        produzir a mensagem
            for (int i=0;i<10;i++){

                String topic = "java_demo";
                String key = "id"+i;
                String value = "Hello World"+i;

                ProducerRecord<String,String> producerRecord =
                        new ProducerRecord<>(topic,key,value);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // se enviar o registro com sucesso ou se der exceção ele a demonstra
                        if (e== null){
                            log.info("Key"+ key +" | Partition: "+metadata.partition());
                        }else{
                            log.error("Error while producing",e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

//      enviar a mensagem produzida

//        limpar e fechar o producer
        producer.flush();
        producer.close();
    }
}