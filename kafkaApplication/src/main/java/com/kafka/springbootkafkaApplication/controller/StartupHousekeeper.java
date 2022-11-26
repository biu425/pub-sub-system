package com.kafka.springbootkafkaApplication.controller;

import com.kafka.springbootkafkaApplication.service.ListenerWorker;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.stereotype.Component;


import java.sql.SQLException;
import java.util.Set;


/* This will be running when start running application
 * automatically add a listener to exited topic and consume posted message
 */

@EnableKafka
@Component
public class StartupHousekeeper {
    @Autowired
    private ConsumerFactory<String, String> topicListenerFactory;

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate; //<topic, message>

    private static String TOPIC_PREFIX = "topicQue_";

    @EventListener(ContextRefreshedEvent.class)
    public void contextRefreshedEvent() throws SQLException {
        ListTopicsResult listTopics = adminClient.listTopics();
        try {
            Set<String> names = listTopics.names().get();
            System.out.println("Checking listener for existed topics...");
            for(String name:names){
                if(name.startsWith(TOPIC_PREFIX)){
                    System.out.println("Add listener to existed topic: "+name);
                    startListening(name);
                }
            }
        }catch (Exception e){
            e.getMessage();
        }
    }

    private String startListening(String topicName){
        try{
            ContainerProperties containerProperties = new ContainerProperties(topicName);
            containerProperties.setMessageListener(new ListenerWorker(this.kafkaTemplate));

            ConcurrentMessageListenerContainer<String, String> container =
                    new ConcurrentMessageListenerContainer<>(
                            topicListenerFactory,
                            containerProperties);
            container.start();

            return "Start listening.";
        }catch (Exception e){
            return e.getMessage();
        }
    }
}

