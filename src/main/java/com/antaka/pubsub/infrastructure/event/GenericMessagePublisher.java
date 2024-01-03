package com.antaka.pubsub.infrastructure.event;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GenericMessagePublisher {

  @Value("${spring.cloud.gcp.project-id}")
  private String projectId;

  public void publishMessage(String topicId, String message) {
    publishMessage(this.projectId, topicId, message);
  }

  public void publishMessage(String projectId, String topicId, String message) {
    TopicName topicName = TopicName.of(projectId, topicId);
    Publisher publisher = null;

    try {
      publisher = Publisher.newBuilder(topicName).build();

      ByteString data = ByteString.copyFromUtf8(message);
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

      ApiFuture<String> future = publisher.publish(pubsubMessage);

      ApiFutures.addCallback(future, new ApiFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
              if (throwable instanceof ApiException apiException) {
                // details on the API exception
                log.error("Unable to publish message, code [{}], message is retryable [{}]",
                    apiException.getStatusCode().getCode(), apiException.isRetryable(), apiException);
              }
              log.error("Unable to publish message, exception ", throwable);
            }

            @Override
            public void onSuccess(String messageId) {
              log.info("Successfully published message with id [{}] on topic [{}]", messageId, topicId);
            }
          },
          MoreExecutors.directExecutor());
    } catch (IOException e) {
      log.error("Unable to create publisher, exception", e);
    } finally {
      if (publisher != null) {
        // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown();
        try {
          publisher.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          log.error("Unable to terminate publisher, it may full the memory. Require investigation");
        }
      }
    }
  }

}
