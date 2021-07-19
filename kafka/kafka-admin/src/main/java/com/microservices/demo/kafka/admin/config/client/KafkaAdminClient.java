package com.microservices.demo.kafka.admin.config.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
@AllArgsConstructor
public class KafkaAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;
    private final WebClient webClient;


    public void createTopics() throws Exception {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopic);
        } catch (Throwable e) {
            throw new RuntimeException("reached Max number of retry for creating the kafka topics");
        }
        checkTopicCreated();
    }

    private CreateTopicsResult doCreateTopic(RetryContext retryContext) {
        List<String> topicName = kafkaConfigData.getTopicNamesToCreate();
        LOG.info("Creating {} topics , attempt {}",topicName.size(),retryContext.getRetryCount());
        List<NewTopic> collect = topicName.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());
        return adminClient.createTopics(collect);
    }
    private Collection<TopicListing> getTopics() throws Exception {
        Collection<TopicListing> topics = null;
        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new Exception("Reached max number of retry for reading kafka topics");
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        LOG.info("Reading kafka topic {}, attempt {}",kafkaConfigData.getTopicNamesToCreate().toArray(),retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if(topics !=null){
            topics.forEach(topic -> LOG.info("Topic with name {}",topic.name()));
        }
        return topics;
    }


    public void checkTopicCreated() throws Exception {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        for(String topic : kafkaConfigData.getTopicNamesToCreate()){
            while (!isTopicCreated(topics,topic)){
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }

    }

    private void sleep(Long sleepTimeMs) throws Exception {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new Exception("Error while sleeping for waiting time");
        }
    }

    private void checkMaxRetry(int retry, Integer maxRetry) throws Exception {
        if(retry  > maxRetry){
            throw new Exception("Reached max number of retry for reading kafka topics");
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topic) {
        if(topics ==null){
            return false;
        }
        return topics.stream().allMatch(topicsList -> topicsList.name().equals(topic));
    }


    public void checkSchemaRegistry() throws Exception {
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        for(String topic : kafkaConfigData.getTopicNamesToCreate()){
            while (getSchemaRegistryStatus().is2xxSuccessful()){
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
            }
        }
    }


    private HttpStatus getSchemaRegistryStatus() {
        try {
            return webClient.method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        } catch (Exception ex) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }


}
