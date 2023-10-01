package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import twitter4j.TwitterException;

import java.io.IOException;
import java.lang.module.InvalidModuleDescriptorException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-mock-tweets} && not ${twitter-to-kafka-service.enable-v2-tweets}") //Mock and APIv2 condition
public class TwitterV2KafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    @Autowired
    private TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    @Autowired
    private TwitterV2StreamHelper twitterV2StreamHelper;


    @Override
    public void start(){
        String bearerToken = twitterToKafkaServiceConfigData.getTwitterV2BearerToken();
        if (null != bearerToken){
            try {
                twitterV2StreamHelper.setupRules(bearerToken, getRules());
                twitterV2StreamHelper.connectStream(bearerToken);
            } catch (IOException | URISyntaxException e) {
                LOG.error("Error streaming tweets!", e);
                throw new RuntimeException("Error streaming tweets!", e);
            }
        } else {
        LOG.error("There was a problem getting your bearer token. " +
                "Please make sure you set the TWITTER_BEARER_TOKEN environement variable.");
        throw new RuntimeException("There was a problem getting your bearer token. +" +
                "Please make sure you set the TWITTER_BEARER_TOKEN environment variable");
        }
    }

    private Map<String, String> getRules() {

        List<String> keywords = twitterToKafkaServiceConfigData.getTwitterKeywords();
        Map<String, String> rules = new HashMap<>();
        for (String keyword: keywords){
            rules.put(keyword, "keyword: " + keyword);
        }
        LOG.info("Created filter for twitter stream for keywords : {}", keywords);
        return rules;
    }
}
