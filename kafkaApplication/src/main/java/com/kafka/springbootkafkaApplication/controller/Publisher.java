package com.kafka.springbootkafkaApplication.controller;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("publish")
public class Publisher {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    //publish a new topic
//    @GetMapping("/publish/newTopic/{topic}")
//    @Bean
//    public NewTopic topicExample(@PathVariable("topic") String newTopic) {
//        System.out.println("New topic created: "+ newTopic);
//        return TopicBuilder.name(newTopic).build();
//    }
//    public String pub(@PathVariable("topic") final String topic){
//        TOPIC = topic;
//        return "Published a new topic: " + topic;
//    }
    @GetMapping("/publish/newTopic/{topic}")
    public String topic1(@PathVariable("topic") String newTopicName) {
        NewTopic newTopic = TopicBuilder.name(newTopicName).build();
        return newTopic.toString();
    }

    //post new message to given topic
    @GetMapping("/publish/post/{topic}/{message}")
    public String post(@PathVariable("message") final String message, @PathVariable("topic") final String topic){
        ListenableFuture<SendResult<String, String>> result = kafkaTemplate.send(topic, message);

        return result.toString();
    }

}
