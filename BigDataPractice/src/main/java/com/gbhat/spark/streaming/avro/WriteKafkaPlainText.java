package com.gbhat.spark.streaming.avro;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/*
sudo docker-compose -f kafka_flink.yml up

To use docker kafka clusters, Run script: add_docker_container_to_hosts.sh **IMPORTANT STEP**

 */

@SpringBootApplication
@EnableScheduling
public class WriteKafkaPlainText {

    static int count = 0;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    public static void main(String[] args) {
        SpringApplication.run(com.gbhat.spark.streaming.avro.WriteKafkaPlainText.class, args);
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("plain_text_topic")
                .partitions(10)
                .replicas(1)
                .build();
    }

    @KafkaListener(id = "myId", topics = "plain_text_topic")
    public void listen(String in) {
        System.out.println(in);
    }

    @Scheduled(fixedRate = 500L)
    public void sendMessage() {
        String data = "User_" + count + "," + count + "," + (10 + count);
        kafkaTemplate.send("plain_text_topic", data);
        count++;
    }
}