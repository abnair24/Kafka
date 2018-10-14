package com.abn.kafka;

import com.abn.kafka.Helper.ConsumerCreator;
import com.abn.kafka.Helper.ProducerCreator;
import com.abn.kafka.constants.KafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

public class TestConsumer {

    public static void main(String[] args) {
        startProducer();
        startConsumer();
    }

    public static void startProducer() {
        Producer<Long, String> producer = ProducerCreator.createProducer();
        for (int index = 0; index < KafkaConstants.MESSAGE_COUNT; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(KafkaConstants.TOPIC_NAME,
                    "This is record " + index);
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            }
            catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
            catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }
    public static void startConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
        int noMessageFound = 0;
        try {
            while (true) {
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

                consumerRecords.forEach(record -> {
                    System.out.println("Record Key " + record.key());
                    System.out.println("Record value " + record.value());
                    System.out.println("Record partition " + record.partition());
                    System.out.println("Record offset " + record.offset());
                });
                consumer.commitAsync();
            }
        }catch(Exception e) {
            System.out.println(e.getMessage());
        } finally {
            consumer.close();
        }
    }

}
