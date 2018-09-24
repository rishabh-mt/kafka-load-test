package com.mindtickle.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);
    private static ConfigManager configManager;
    private String propPath = "resources/app.properties";
    private final Properties properties;

    private String kafkaBootstrapServers;
    private static final String TOPIC_PREFIX = "loadtest-";

    public static ConfigManager getInstance() {
        if(configManager == null) {
            synchronized (ConfigManager.class) {
                if(configManager == null) {
                    configManager = new ConfigManager();
                }
            }
        }
        return  configManager;
    }


    private ConfigManager() {
        properties = new Properties();
        loadConfig();
    }

    private void loadConfig() {
        // Load properties from config file
        logger.info("Reading config from file - <{}> ", propPath);
        try {
            File file = new File(propPath);
            FileInputStream fileInput = new FileInputStream(file);
            properties.load(fileInput);
        } catch (IOException e) {
            logger.error("Exception while loading config file", e);
            // Throwing run time exception to terminate the JVM since it is not possible to proceed without config
            throw new RuntimeException(e);
        }

        // Assign properties for easy use
        kafkaBootstrapServers = properties.getProperty("kafka.bootstrap.servers");

    }

    // Avoid using this method
    public Properties getProperties() {
        return properties;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getTopicPrefix() { return TOPIC_PREFIX; }
}
