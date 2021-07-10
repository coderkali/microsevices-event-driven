package com.microservices.demo.twitter.to.kafka.runner;

import com.microservices.demo.twitter.to.kafka.config.TwitterToKafkaConfig;
import com.microservices.demo.twitter.to.kafka.listener.TwitterToKafkaListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweet",havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final TwitterToKafkaConfig twitterToKafkaConfig;

    private final TwitterToKafkaListener twitterToKafkaListener;

    private TwitterStream twitterStream;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[] {
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetuer",
            "adipiscing",
            "elit",
            "Maecenas",
            "porttitor",
            "conque",
            "massa",
            "Fusce",
            "posuere",
            "magna",
            "sed",
            "pulvinar",
            "ulticies",
            "purus",
            "lectus",
            "malesuada",
            "libero"
    };

    private static final String tweetAsRawJson = "{"
            + "\"created_at\":\"{0}\","
            + "\"id\": \"{1}\","
            + "\"text\": \"{2}\","
            + "\"user\": {\"id\": \"{3}\"}"
            + "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterToKafkaConfig twitterToKafkaConfig, TwitterToKafkaListener twitterToKafkaListener) {
        this.twitterToKafkaConfig = twitterToKafkaConfig;
        this.twitterToKafkaListener = twitterToKafkaListener;
    }

    @Override
    public void start() throws TwitterException {
        String[] keyWords = twitterToKafkaConfig.getTwitterKeyword().toArray(new String[0]);
        int minTweetLength = twitterToKafkaConfig.getMockMinTweetLength();
        int maxTweetLength = twitterToKafkaConfig.getMockMaxTweetLength();
        long sleepTimeMs = twitterToKafkaConfig.getMockSleepMs();
        LOG.info("Starting mock filtering twitter streams for keywords: {}", Arrays.toString(keyWords));

        simulateTwitterStream(keyWords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keyWords, int minTweetLength, int maxTweetLength, long sleepTimeMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            try{
                while(true){
                    String formattedTwitterJson = getFormattedTweet(keyWords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTwitterJson);
                    twitterToKafkaListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            }catch(TwitterException e){
                e.printStackTrace();;
                LOG.error("Error creating twitter status ",e);
            }

        });

    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String getFormattedTweet(String[] keyWords, int minTweetLength, int maxTweetLength) {
        String [] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keyWords,minTweetLength,maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return formatTweetJsonWIthParam(params);
    }

    private String formatTweetJsonWIthParam(String[] params) {
        String tweet = tweetAsRawJson;
        for(int i = 0; i< params.length; i++){
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keyWords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keyWords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keyWords, StringBuilder tweet, int tweetLength) {
        for(int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength / 2) {
                tweet.append(keyWords[RANDOM.nextInt(keyWords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}
