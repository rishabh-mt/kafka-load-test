package com.mindtickle.load;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public abstract class AbstractLoadGeneratorThread<V> implements Runnable {

    protected KafkaProducer<String, V> kafkaProducer;

    public abstract void initKafkaProducer();

    public abstract ProducerRecord<String, V> generatePayload();

    public void startProducing() {
        initKafkaProducer();
        while (true) {
            ProducerRecord<String, V> record = generatePayload();
            System.out.println(Thread.currentThread() + "Produced Record:" + record);
            kafkaProducer.send(record);
        }
    }

    public void run() {
        startProducing();
    }
}
