package com.kafka.springbootkafkaApplication.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class ListenerWorker implements MessageListener<String, String> {
    private final KafkaTemplate<String, String> kafkaTemplate;

    private String fromTopic;
    private String toTopic;
    private String receivedMsg;
    private static String SUB_PREIFX = "subQue_";

    public ListenerWorker(KafkaTemplate<String, String> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    //check topic subscription in DB, send to subscribers' queue
    //consumerRecord.value is the posted message
    public void onMessage(ConsumerRecord<String, String> consumerRecord) {
        fromTopic = consumerRecord.topic();
        receivedMsg = consumerRecord.value();
        System.out.println("********listener received: " + fromTopic  + " " + receivedMsg);
        System.out.println();


        //TODO: get subscription from DB
        toTopic = "subQue_sub2";

        //send msg to subscribers
        ListenableFuture<SendResult<String, String>> result = kafkaTemplate.send(toTopic, receivedMsg);
        result.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onFailure(Throwable ex) {
                System.out.printf("The record cannot be processed! caused by %s", ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Success resend in: " + result.toString());
            }
        });
    }
}
