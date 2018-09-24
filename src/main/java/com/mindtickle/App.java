package com.mindtickle;

import com.mindtickle.config.ConfigManager;
import com.mindtickle.load.LoadGeneratorThread;
import com.mindtickle.manager.KafkaStateManager;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {

    private static void generateLoad(int numThreads, int topics, int partitions) {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        for(int i=0; i<numThreads; i++) {
            String topic = ConfigManager.getInstance().getTopicPrefix() + new Random().nextInt(topics);
            int partition = new Random().nextInt(partitions);
            System.out.println(String.format("Generating load for t=%s p=%s", topic, partition));
            executorService.execute(new LoadGeneratorThread(topic, partition));
        }
        try {
            executorService.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }

    }

    public static void main(String[] args) {
        // TODO Use logger instead of sysout and printStackTrace
        ConfigManager.getInstance();
        System.out.println("Starting now:" + System.currentTimeMillis());
        KafkaStateManager ksm = new KafkaStateManager();
        int topics = 10;
        int partitions = 1500;
        int numThreads = 10;
        try {
            //ksm.reset();
            ksm.initState(topics, partitions);
            // generateLoad(numThreads, topics, partitions);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ksm.close();
        }
    }
}
