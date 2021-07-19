package com.microservices.demo.twitter.to.kafka.transformer;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.springframework.stereotype.Component;
import twitter4j.Status;


@Component
public class TwitterStatusToAvroTransformer {
    public TwitterAvroModel getTwitterAvroModelFromStatus(Status status) {
        return TwitterAvroModel.newBuilder()
                .setId(status.getId())
                .setUserId(status.getUser().getId())
                .setText(status.getText())
                .setCreatedAt(String.valueOf(status.getCreatedAt().getTime()))
                .build();
    }
}