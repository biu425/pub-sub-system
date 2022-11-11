package com.kafka.springbootkafkaApplication.controller;

import com.kafka.springbootkafkaApplication.service.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("subscribe")
public class TopicListener {
    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    //fetch message from given topic
    @GetMapping("/fetch/{topic}")
    public String fetch(@PathVariable("topic") final String topic){
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener(new KafkaConsumer());

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(
                        consumerFactory,
                        containerProperties);
        container.start();

        return "Message consumed.";
    }
}
