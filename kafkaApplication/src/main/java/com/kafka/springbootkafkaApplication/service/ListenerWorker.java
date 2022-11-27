package com.kafka.springbootkafkaApplication.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ListenerWorker implements MessageListener<String, String> {
    private final KafkaTemplate<String, String> kafkaTemplate;

    private String fromTopic;
    private String receivedMsg;
    private Connection conn;

    public ListenerWorker(KafkaTemplate<String, String> kafkaTemplate, Connection conn){
        this.kafkaTemplate = kafkaTemplate;
        this.conn = conn;
    }


    //check topic subscription in DB, send to subscribers' queue
    //consumerRecord.value is the posted message
    public void onMessage(ConsumerRecord<String, String> consumerRecord) {
        fromTopic = consumerRecord.topic();
        receivedMsg = consumerRecord.value();
        System.out.println("********listener received: " + fromTopic  + " " + receivedMsg);
        System.out.println();


        //get subscription from DB
        String selectSQL = "SELECT DISTINCT subscriber FROM subscription WHERE topicName = \""+fromTopic+"\"";
        System.out.println(selectSQL);
        List<String> subscribers = new ArrayList<>();

        try {
            ResultSet subscriptionResults = conn.createStatement().executeQuery(selectSQL);
            while(subscriptionResults.next()){
                String subscriber = subscriptionResults.getString(1);
                System.out.println("selected subscriber: " + subscriber);
                subscribers.add(subscriber);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        //send msg to subscribers
        for(String toTopic:subscribers){
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
}
