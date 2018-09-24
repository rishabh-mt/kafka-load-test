package com.mindtickle.manager;

import com.mindtickle.config.ConfigManager;
import org.apache.kafka.clients.admin.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaStateManager {

    private static final AdminClient kafkaAdminClient = createAdminKafktaClient();

    private static AdminClient createAdminKafktaClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getInstance().getKafkaBootstrapServers());
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10 * 60 * 1000); // Setting time out to 10mins
        properties.setProperty("client.id", "consumerAdmin");
        properties.setProperty("metadata.max.age.ms", "3000");
        return AdminClient.create(properties);
    }

    private boolean filter(String topic) {
        if(topic.startsWith(ConfigManager.getInstance().getTopicPrefix())) {
            return true;
        } else {
            return false;
        }
    }

    public void reset() throws ExecutionException, InterruptedException {
        ListTopicsResult result = kafkaAdminClient.listTopics();
        Set<String> topicsToBeDeleted = new HashSet<>();
        System.out.println("Getting Kafka topics");
        Collection<TopicListing> listings = result.listings().get();
        System.out.println("Get call returned");
        for(TopicListing listing: listings) {
            System.out.println("Listing=" + listing);
            if(filter(listing.name())) {
                topicsToBeDeleted.add(listing.name());
            }
        }
        System.out.println(System.currentTimeMillis() + "Topics to be deleted:" + topicsToBeDeleted);
        kafkaAdminClient.deleteTopics(topicsToBeDeleted).all().get();
        System.out.println(System.currentTimeMillis() + "Topic deletion complete");
    }

    public void initState(int topics, int partitions) throws ExecutionException, InterruptedException {
        int index = 1;
        Set<NewTopic> topicsToBeCreated = new HashSet<>();
        while(index<=topics) {
            // topicsToBeCreated.clear();
            NewTopic newTopic = new NewTopic(ConfigManager.getInstance().getTopicPrefix()+ index, partitions, Short.valueOf("2"));
            topicsToBeCreated.add(newTopic);
            /*
            System.out.println(System.currentTimeMillis() + "Topics to be created::" + topicsToBeCreated);
            kafkaAdminClient.createTopics(topicsToBeCreated).all().get();
            System.out.println(System.currentTimeMillis() + "Topic creation complete");
            */
            index++;
        }
        System.out.println(System.currentTimeMillis() + "Topics to be created::" + topicsToBeCreated);
        kafkaAdminClient.createTopics(topicsToBeCreated).all().get();
        System.out.println(System.currentTimeMillis() + "Topic creation complete");
    }

    public void close() {
        System.out.println("Closing Kafka Admin Client");
        kafkaAdminClient.close();
    }
}
