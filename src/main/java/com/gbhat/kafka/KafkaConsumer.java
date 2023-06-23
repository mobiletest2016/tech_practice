package com.gbhat.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;

/*

sudo docker-compose -f kafka_flink.yml up

To use docker kafka clusters, Run script: add_docker_container_to_hosts.sh

Test from console producer:
sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092 --topic mytopic

 */

@SpringBootApplication
public class KafkaConsumer {

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumer.class, args);
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("mytopic")
                .partitions(10)
                .replicas(1)
                .build();
    }

    @KafkaListener(id = "myId", topics = "mytopic")
    public void listen(String in) {
        System.out.println(in);
    }

}