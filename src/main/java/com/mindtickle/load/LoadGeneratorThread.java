package com.mindtickle.load;

import com.fasterxml.jackson.databind.JsonNode;
import com.mindtickle.config.ConfigManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;

public class LoadGeneratorThread extends AbstractLoadGeneratorThread<JsonNode> {

    private String topic;
    private int partition;

    public LoadGeneratorThread(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public void initKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getInstance().getKafkaBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "LOAD_TEST_" + System.currentTimeMillis());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        kafkaProducer = new KafkaProducer<String, JsonNode>(props);
    }

    @Override
    public ProducerRecord<String, JsonNode> generatePayload() {
        String key = "";
        JsonNode value = null;
        ProducerRecord<String, JsonNode> record = new ProducerRecord<>(topic, partition, key, value);
        return record;
    }
}
