package com.microservices.demo.twitter.to.kafka.service;

import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Scope;
import org.springframework.context.event.ApplicationListenerMethodAdapter;

import java.util.Arrays;


@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo.twitter.to.kafka.service")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
    @Autowired
    private TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    @Autowired
    private StreamRunner streamRunner;

    public static void main(String[] args) { SpringApplication.run(TwitterToKafkaServiceApplication.class, args); }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("App start...");
        LOG.info(Arrays.toString(twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[] {})));
        LOG.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
        streamRunner.start();
    }
}
