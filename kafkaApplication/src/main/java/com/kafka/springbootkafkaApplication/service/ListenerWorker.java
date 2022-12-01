package com.kafka.springbootkafkaApplication.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.kafka.springbootkafkaApplication.DistributedLock.Lock;
import com.kafka.springbootkafkaApplication.ElectableNode.ElectableNode;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListenerWorker implements MessageListener<String, String> {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private ElectableNode eNode = null;
    private Lock readLock = null;

    /*
     * Set useLock to true if want to use distributed lock to read database.
     * Set useLock to false if want to use leader to read database.
     */
    private final static boolean useLock = true;

    private String fromTopic;
    private String receivedMsg;
    private Connection conn;

    public ListenerWorker(KafkaTemplate<String, String> kafkaTemplate, Connection conn, String topic){
        this.kafkaTemplate = kafkaTemplate;
        this.conn = conn;
        if (!useLock) {
            try {
                eNode = new ElectableNode(topic, conn);
                eNode.connect();
                eNode.selfVolunteer();
                eNode.elect();
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        }
        else {
            readLock = new Lock();
        }
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

        if (useLock) {
            try {
                readLock.connect("read");
                readLock.tryLock();
                System.out.println("Topic " + consumerRecord.topic() + " has the lock.");
                ResultSet subscriptionResults = conn.createStatement().executeQuery(selectSQL);
                while(subscriptionResults.next()){
                    String subscriber = subscriptionResults.getString(1);
                    System.out.println("selected subscriber: " + subscriber);
                    subscribers.add(subscriber);
                }
                readLock.releaseLock();
                System.out.println("Topic " + consumerRecord.topic() + " released the lock.");
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        }
        else {
            if (eNode.isLeader()) {
                try {
                    System.out.println("Message listener for topic " + consumerRecord.topic() + " is leader.");
                    System.out.println("Directly query the database.");
                    ResultSet subscriptionResults = conn.createStatement().executeQuery(selectSQL);
                    while(subscriptionResults.next()){
                        String subscriber = subscriptionResults.getString(1);
                        System.out.println("selected subscriber: " + subscriber);
                        subscribers.add(subscriber);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
                try {
                    System.out.println("Message listener for topic " + consumerRecord.topic() + " is follower.");
                    System.out.println("Send the query to leader.");
                    Socket socket = new Socket("127.0.0.1", 1000);
                    BufferedReader buffer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter pStream = new PrintWriter(socket.getOutputStream(), true);

                    pStream.println(selectSQL);

                    String response = null;
                    
                    while (response == null) {
                        response = buffer.readLine();
                    }

                    System.out.println("reponse received: " + response);

                    if (response.equals("OK")) {
                        String result = buffer.readLine();
                        subscribers = Arrays.asList(result.split(","));
                    }

                    socket.close();
                    buffer.close();
                    pStream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
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
