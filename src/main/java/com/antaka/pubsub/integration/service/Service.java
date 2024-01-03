package com.antaka.pubsub.integration.service;

import com.antaka.pubsub.infrastructure.event.GenericMessagePublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

@org.springframework.stereotype.Service
@RequiredArgsConstructor
@Slf4j
public class Service {

  @Value("${topics.name}")
  String topicName;
  private final GenericMessagePublisher messagePublisher;

  public void sendMessage(String message) {
    log.info("Publish message to topic {}", topicName);
    messagePublisher.publishMessage(topicName, message);
  }
}
