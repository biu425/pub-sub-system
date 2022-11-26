package com.kafka.springbootkafkaApplication.controller;

import com.kafka.springbootkafkaApplication.model.DBUpdate;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

@RestController
@RequestMapping("subscriber")
public class Subscriber {

    private static final String SUB_PREIFX = "subQue_";
    private static final String TOPIC_PREFIX = "topicQue_";
    private Consumer<String, String> subQueConsumer;
    private String topicNameWithPrefix;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate; //<topic, message>

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private ConsumerFactory<String, String> subscriberFactory;

    @Autowired
    private DBUpdate newSubscriptionUpdate;

    //create a new subscriber
    //actually create a new topic queue for this subscriber to que message with subscription
    @GetMapping("/newSubscriber/{name}")
    public String createSubscriber(@PathVariable("name") String name){
        String subNameWithPrefix = SUB_PREIFX + name;
        System.out.printf("*****in createNewSubscriber: /newSubscriber/%s\n", subNameWithPrefix);

        NewTopic newTopic = TopicBuilder.name(subNameWithPrefix).build();
        Collection<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(newTopic);
        CreateTopicsResult result = adminClient.createTopics(newTopics);
        try {
            return result.topicId(subNameWithPrefix).get().toString();

        }catch (Exception e){
            return "Create subscriber failed: "+ e.getMessage();
        }

    }

    //get all available topic
    @GetMapping("/getTopics")
    public String getTopics(){
        StringBuilder list = new StringBuilder();
        ListTopicsResult listTopics = adminClient.listTopics();
        try {
            Set<String> names = listTopics.names().get();
            for(String n:names) {
                if(n.startsWith(TOPIC_PREFIX)) {
                    String name = removePrefix(n);
                    list.append(name).append(" ");
                }
            }
        }catch (Exception e){
            return e.getMessage();
        }
        return list.toString();
    }

    //create new subscription
    @GetMapping("/subscribe/{sub_name}/{topic}")
    public String subscribe(@PathVariable("sub_name") String sub_name, @PathVariable("topic") String topicName) throws SQLException, IOException {
        boolean contains;
        ListTopicsResult listTopics = adminClient.listTopics();
        String topicNameWithPrefix = TOPIC_PREFIX + topicName;
        String subNameWithPrefix = SUB_PREIFX + sub_name;
        try {
            Set<String> names = listTopics.names().get();
            contains = names.contains(topicNameWithPrefix);
        }catch (Exception e){
            return e.getMessage();
        }

        if(!contains){
            return "topic not exist.";
        }else{
            //TODO: update DB for this new subscription
            newSubscriptionUpdate.update(topicNameWithPrefix, subNameWithPrefix);
            System.out.println("Subscribing topic: " + topicName + " for subscriber: " + sub_name);
        }
        return "successfully subscribed to topic: " + topicName;
    }

    //pull all existed messages from subscription
    //create consumer for this subQue and consume all unread message
    @GetMapping("/{sub_name}/poll")
    public String pollUnreadMsg(@PathVariable("sub_name") String sub_name){
        //create consumer of the subQue to poll all unread msg
        //close consumer after finishing
        String subNameWithPrefix = SUB_PREIFX + sub_name;

        subQueConsumer = subscriberFactory.createConsumer();
        subQueConsumer.subscribe(Arrays.asList(subNameWithPrefix));

        StringBuilder msgList = new StringBuilder();
        msgList.append("Got messages:\n");
        try {
            while (true) {
                ConsumerRecords<String, String> records = subQueConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.printf("Subscriber polling messages: topic = %s, partition = %d, offset = %d, message = %s",record.topic(), record.partition(), record.offset(), record.value());
                    System.out.println();
                    msgList.append("\n"+record.value());
                }
                return msgList +"\n";
            }
        } finally {
            subQueConsumer.close();
        }
    }

    //remove prefix from subNameWithPrefix to get the
    //topic name for client
    //split[]: [prefix, name]
    private String removePrefix(String subNameWithPrefix){
        String[] split = subNameWithPrefix.split("_");
        return split[1];
    }
}
