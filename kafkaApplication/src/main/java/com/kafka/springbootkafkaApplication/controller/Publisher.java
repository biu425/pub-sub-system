package com.kafka.springbootkafkaApplication.controller;

import com.kafka.springbootkafkaApplication.service.ListenerWorker;
import org.apache.kafka.clients.admin.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Controller;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

@RestController
@Controller
@RequestMapping("publisher")
public class Publisher {
    private static String TOPIC_PREFIX = "topicQue_";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate; //<topic, message>

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private ConsumerFactory<String, String> topicListenerFactory;

    @Autowired
    private Connection conn;

    //publish a new topic
    @GetMapping("/newTopic/{topic}")
    public String createNewTopic(@PathVariable("topic") String newTopicName) {
        String topicNameWithPrefix = TOPIC_PREFIX + newTopicName;
        System.out.printf("*****in createTopic: /newTopic/%s\n", topicNameWithPrefix);

        NewTopic newTopic = TopicBuilder.name(topicNameWithPrefix).build();
        Collection<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(newTopic);
        CreateTopicsResult result = adminClient.createTopics(newTopics);
        try {
            //start listener
            String listenerStatus = startListening(topicNameWithPrefix);

            return result.topicId(topicNameWithPrefix).get().toString() + " " + listenerStatus;

        }catch (Exception e){
            return "Create topic failed: "+ e.getMessage();
        }
    }

    private String startListening(String topicName){
        try{
            ContainerProperties containerProperties = new ContainerProperties(topicName);
            containerProperties.setMessageListener(new ListenerWorker(this.kafkaTemplate, this.conn));

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


    //post new message to given topic
    @GetMapping("/post/{topic}/{message}")
    public String post(@PathVariable("message") String message, @PathVariable("topic") String topic){
        String topicNameWithPrefix = TOPIC_PREFIX+ topic;
        System.out.printf("*****in post: /post/%s/%s\n", topic, message);

        ListTopicsResult listTopics = adminClient.listTopics();
        boolean contains;
        try {
            Set<String> names = listTopics.names().get();
            for(String n:names){
                System.out.println(n);
            }
            contains = names.contains(topicNameWithPrefix);
        }catch (Exception e){
            return e.getMessage();
        }

        if(!contains){
            return "topic not exist";
        }
        ListenableFuture<SendResult<String, String>> result = kafkaTemplate.send(topicNameWithPrefix, message);
        result.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onFailure(Throwable ex) {
                System.out.printf("The record cannot be processed! caused by %s", ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Success post in: " + result.toString());
            }
        });

        return result.toString();
    }
}
