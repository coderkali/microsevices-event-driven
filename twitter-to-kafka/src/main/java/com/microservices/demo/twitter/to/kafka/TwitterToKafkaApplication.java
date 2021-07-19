package com.microservices.demo.twitter.to.kafka;

import com.microservices.demo.config.TwitterToKafkaConfig;
import com.microservices.demo.twitter.to.kafka.init.StreamInitializer;
import com.microservices.demo.twitter.to.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;


@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaApplication.class);


    private final StreamRunner streamRunner;

    private final StreamInitializer streamInitializer;

    public TwitterToKafkaApplication(StreamRunner streamRunner,StreamInitializer streamInitializer) {
        this.streamRunner = streamRunner;
        this.streamInitializer = streamInitializer;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplication.class);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("APP Started....");
        streamInitializer.init();
        streamRunner.start();
    }
}
