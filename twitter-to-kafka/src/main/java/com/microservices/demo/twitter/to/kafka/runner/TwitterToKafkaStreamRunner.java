package com.microservices.demo.twitter.to.kafka.runner;

import com.microservices.demo.config.TwitterToKafkaConfig;
import com.microservices.demo.twitter.to.kafka.listener.TwitterToKafkaListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweet",havingValue = "false",matchIfMissing = false)
public class TwitterToKafkaStreamRunner implements  StreamRunner{

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaStreamRunner.class);

    private final TwitterToKafkaConfig twitterToKafkaConfig;

    private final TwitterToKafkaListener twitterToKafkaListener;

    private TwitterStream twitterStream;

    public TwitterToKafkaStreamRunner(TwitterToKafkaConfig twitterToKafkaConfig, TwitterToKafkaListener twitterToKafkaListener) {
        this.twitterToKafkaConfig = twitterToKafkaConfig;
        this.twitterToKafkaListener = twitterToKafkaListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterToKafkaListener);
        addFilter();
    }
    @PreDestroy
    public void shutDown(){
       if(twitterStream!=null){
           LOG.info("closing twitter stream!");
           twitterStream.shutdown();
       }
    }

    private void addFilter() {
        String[] keyWords = twitterToKafkaConfig.getTwitterKeyword().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keyWords);
        twitterStream.filter(filterQuery);
        LOG.info("Started filtering twitter stream for keywords {}",Arrays.toString(keyWords));
    }
}
