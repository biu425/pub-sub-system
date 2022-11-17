package com.kafka.springbootkafkaApplication.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;


public class ListenerWorker implements MessageListener<String, String> {

//    @KafkaListener(topics = "kafka_example")
//    public void consume(String message) {
//        System.out.println("Consumed message: " + message);
//    }

    //TODO: check topic subscription in DB, send to subscribers' queue
    //consumerRecord.value is the posted message
    public void onMessage(ConsumerRecord<String, String> consumerRecord) {
//        if (!this.filter(consumerRecord)) {
//            ((MessageListener)this.delegate).onMessage(consumerRecord);
//        }
        System.out.println("********consumer received: " + consumerRecord.topic() + " " + consumerRecord.value());
        System.out.println();
    }
}
