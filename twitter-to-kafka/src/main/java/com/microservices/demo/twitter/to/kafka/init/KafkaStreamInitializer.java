package com.microservices.demo.twitter.to.kafka.init;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.kafka.admin.config.client.KafkaAdminClient;
import com.microservices.demo.kafka.producer.config.service.impl.TwitterKafkaProducer;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class KafkaStreamInitializer implements StreamInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);
    private final KafkaConfigData kafkaConfigData;
    private final KafkaAdminClient kafkaAdminClient;


    @Override
    public void init() {
        try {
        kafkaAdminClient.createTopics();
        kafkaAdminClient.checkSchemaRegistry();
        LOG.info("Topic with the name {} is ready for operations!",
                kafkaConfigData.getTopicNamesToCreate().toArray());
        } catch (Exception ex) {
            LOG.error("An error occurred ", ex);
        }
    }
}
