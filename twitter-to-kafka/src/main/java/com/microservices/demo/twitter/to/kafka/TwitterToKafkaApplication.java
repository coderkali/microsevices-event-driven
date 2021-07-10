package com.microservices.demo.twitter.to.kafka;

import com.microservices.demo.twitter.to.kafka.config.TwitterToKafkaConfig;
import com.microservices.demo.twitter.to.kafka.runner.StreamRunner;
import com.microservices.demo.twitter.to.kafka.runner.TwitterToKafkaStreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;


@SpringBootApplication
public class TwitterToKafkaApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaApplication.class);

    TwitterToKafkaConfig twitterToKafkaConfig;

    StreamRunner streamRunner;

    public TwitterToKafkaApplication(TwitterToKafkaConfig twitterToKafkaConfig,StreamRunner streamRunner) {
        this.twitterToKafkaConfig = twitterToKafkaConfig;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplication.class);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("APP Started....");
        LOG.info(twitterToKafkaConfig.getWelcomeMessage());
        LOG.info(Arrays.toString(twitterToKafkaConfig.getTwitterKeyword().toArray(new String[] {})));
        streamRunner.start();
    }
}
