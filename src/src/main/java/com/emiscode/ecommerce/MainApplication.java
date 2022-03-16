package com.emiscode.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MainApplication {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var producer = new KafkaProducer<String, String>(properties())) {
            var value = "100,200,300";
            var producerRecord = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);

            producer.send(producerRecord, (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                Logger.getGlobal().log(
                        Level.INFO,
                        String.format(
                                "[SUCCESS] [topic] %s [partition] %s [offset] %s [timestamp] %s",
                                data.topic(),
                                data.partition(),
                                data.offset(),
                                data.timestamp()
                        )
                );
            }).get();
        }

    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
