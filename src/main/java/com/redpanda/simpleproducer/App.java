package com.redpanda.simpleproducer;

import java.io.*;
import java.util.Properties;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.lang.Thread;
import java.io.BufferedWriter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
// import org.apache.kafka.common.errors.TimeoutException;

public class App {
    public static void main( String[] args ) {
        final String brokers = args.length > 0 ? args[0] : "localhost:9092";
        final String topic = args.length > 1 ? args[1] : "some-topic";
        final long runtime_ms = args.length > 2 ? Long.parseLong(args[2]) * 1000 : 60000;
        final String compression = args.length > 3 ? args[3] : "none";

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        if (!Objects.equals(compression, "none")) {
            System.out.println("Using compression type " + compression);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        }

        Producer<String, String> producer = new KafkaProducer<>(props);
        long record_count = 0;
        long end_time = System.currentTimeMillis() + runtime_ms;

        while (System.currentTimeMillis() < end_time) {
            producer.send(new ProducerRecord<String, String>(topic, "key-" + record_count, "value-" + record_count));
            System.out.println("Sent record " + record_count);
            record_count += 1;
        }

        producer.close();
    }
}
