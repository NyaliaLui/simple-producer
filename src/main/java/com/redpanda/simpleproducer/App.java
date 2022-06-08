package com.redpanda.simpleproducer;

import java.io.*;
import java.util.Properties;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.Random;
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
        long end_time = System.currentTimeMillis() + runtime_ms;
        long known_offset = -1;

        while (System.currentTimeMillis() < end_time) {
            try {
                // Let a value be a random sequence of 512 bytes
                byte[] value_bytes = new byte[512];
                new Random().nextBytes(value_bytes);
                String value = new String(value_bytes);

                var f = producer.send(new ProducerRecord<String, String>(topic, "key-" + known_offset, value));
                var m = f.get();
                known_offset = Math.max(known_offset, m.offset());
                System.out.println("Sent record " + known_offset);
            } catch (Exception ex) {
                System.out.println(ex);
                ex.printStackTrace();
            }
        }

        producer.close();
    }
}
