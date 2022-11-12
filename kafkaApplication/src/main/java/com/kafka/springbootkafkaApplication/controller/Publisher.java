package com.kafka.springbootkafkaApplication.controller;

import org.apache.kafka.clients.admin.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

@RestController
@RequestMapping("publisher")
public class Publisher {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private AdminClient adminClient;

    //publish a new topic
    @GetMapping("/newTopic/{topic}")
    public String createNewTopic(@PathVariable("topic") String newTopicName) {
        System.out.printf("*****in createTopic: /newTopic/%s\n", newTopicName);
        NewTopic newTopic = TopicBuilder.name(newTopicName).build();
        Collection<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(newTopic);
        CreateTopicsResult result = adminClient.createTopics(newTopics);
        try {
            return result.topicId(newTopicName).get().toString();
        }catch (Exception e){
            return e.getMessage();
        }
    }

    //TODO: update DB with new topic

    //post new message to given topic
    @GetMapping("/post/{topic}/{message}")
    public String post(@PathVariable("message") String message, @PathVariable("topic") String topic){
        System.out.printf("*****in post: /post/%s/%s\n", topic, message);
        ListTopicsResult listTopics = adminClient.listTopics();
        boolean contains;
        try {
            Set<String> names = listTopics.names().get();
            for(String n:names){
                System.out.println(n);
            }
            contains = names.contains(topic);
        }catch (Exception e){
            return e.getMessage();
        }

        if(!contains){
            return "topic not exist";
        }
        ListenableFuture<SendResult<String, String>> result = kafkaTemplate.send(topic, message);
        result.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

                @Override
                public void onFailure(Throwable ex) {
                    System.out.printf("The record cannot be processed! caused by %s", ex.getMessage());
                }

                @Override
                public void onSuccess(SendResult<String, String> result) {
                    System.out.println("Success");
                }
            });

        return result.toString();
    }
}
