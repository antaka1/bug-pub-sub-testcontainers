package com.antaka.pubsub.infrastructure.http;

import static org.springframework.http.HttpHeaders.CONTENT_TYPE;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT
)
@AutoConfigureWebTestClient
@TestInstance(Lifecycle.PER_CLASS)
@DirtiesContext(
    classMode = ClassMode.AFTER_CLASS
)
@Testcontainers
class IntegrationTest {

  @Autowired
  protected WebTestClient client;


  private final static String PROJECT_ID = "integration-project";

  @Container
  // emulatorContainer {
  public PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(
      DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators")

  );

//  @DynamicPropertySource
//  static void emulatorProperties(DynamicPropertyRegistry registry) throws InterruptedException {
//    pubsubEmulator.start();
//    registry.add("spring.cloud.gcp.pubsub.emulator-host", pubsubEmulator::getEmulatorEndpoint);
//  }
//
//  @BeforeAll
//  static void setup() throws Exception {
//    ManagedChannel channel =
//        ManagedChannelBuilder.forTarget("dns:///" + pubsubEmulator.getEmulatorEndpoint())
//            .usePlaintext()
//            .build();
//    TransportChannelProvider channelProvider =
//        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
//
//    TopicAdminClient topicAdminClient =
//        TopicAdminClient.create(
//            TopicAdminSettings.newBuilder()
//                .setCredentialsProvider(NoCredentialsProvider.create())
//                .setTransportChannelProvider(channelProvider)
//                .build());
//
//    SubscriptionAdminClient subscriptionAdminClient =
//        SubscriptionAdminClient.create(
//            SubscriptionAdminSettings.newBuilder()
//                .setTransportChannelProvider(channelProvider)
//                .setCredentialsProvider(NoCredentialsProvider.create())
//                .build());
//
//    PubSubAdmin admin =
//        new PubSubAdmin(() -> PROJECT_ID, topicAdminClient, subscriptionAdminClient);
//
//    admin.createTopic("test-topic");
//    admin.createSubscription("test-subscription", "test-topic");
//    admin.close();
//    channel.shutdown();
//  }

  // By default, autoconfiguration will initialize application default credentials.
  // For testing purposes, don't use any credentials. Bootstrap w/ NoCredentialsProvider.
//  @TestConfiguration
//  static class PubSubEmulatorConfiguration {
//
//    @Bean
//    CredentialsProvider googleCredentials() {
//      return NoCredentialsProvider.create();
//    }
//  }


  @Test
  void testSimple() throws IOException {
    String hostport = emulator.getEmulatorEndpoint();
    ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
    try {
      TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(
          GrpcTransportChannel.create(channel)
      );
      NoCredentialsProvider credentialsProvider = NoCredentialsProvider.create();

      String topicId = "my-topic-id";
      createTopic(topicId, channelProvider, credentialsProvider);

      String subscriptionId = "my-subscription-id";
      createSubscription(subscriptionId, topicId, channelProvider, credentialsProvider);

      client.post().uri("/message")
          .body((BodyInserters.fromValue("test-message")))
          .header(CONTENT_TYPE, "application/json")
          .exchange()
          .expectStatus().is2xxSuccessful();

      SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings
          .newBuilder()
          .setTransportChannelProvider(channelProvider)
          .setCredentialsProvider(credentialsProvider)
          .build();
      try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
        PullRequest pullRequest = PullRequest
            .newBuilder()
            .setMaxMessages(1)
            .setSubscription(ProjectSubscriptionName.format(PROJECT_ID, subscriptionId))
            .build();
        PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

        Assertions.assertThat(pullResponse.getReceivedMessagesList()).hasSize(1);
        Assertions.assertThat(pullResponse.getReceivedMessages(0).getMessage().getData().toStringUtf8())
            .isEqualTo("test message");
      }
    } finally {
      channel.shutdown();
    }
  }

  private String readMessage(String projectId, String subscriptionId) throws IOException {
    SubscriberStubSettings subscriberStubSettings =
        SubscriberStubSettings.newBuilder()
            .setTransportChannelProvider(
                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                    .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                    .build())
            .build();

    try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
      String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
      PullRequest pullRequest =
          PullRequest.newBuilder()
              .setMaxMessages(1)
              .setSubscription(subscriptionName)
              .build();

      // Use pullCallable().futureCall to asynchronously perform this operation.
      PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

      // Stop the program if the pull response is empty to avoid acknowledging
      // an empty list of ack IDs.
      if (pullResponse.getReceivedMessagesList().isEmpty()) {
        System.out.println("No message was pulled. Exiting.");
        return "";
      }

      List<String> ackIds = new ArrayList<>();
      for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
        // Handle received message
        // ...
        ackIds.add(message.getAckId());
      }

      // Acknowledge received messages.
      AcknowledgeRequest acknowledgeRequest =
          AcknowledgeRequest.newBuilder()
              .setSubscription(subscriptionName)
              .addAllAckIds(ackIds)
              .build();

      // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
      subscriber.acknowledgeCallable().call(acknowledgeRequest);
      System.out.println(pullResponse.getReceivedMessagesList());
      return pullResponse.getReceivedMessagesList().get(0).getMessage().getData().toStringUtf8();
    }
  }

  private void createTopic(
      String topicId,
      TransportChannelProvider channelProvider,
      NoCredentialsProvider credentialsProvider
  ) throws IOException {
    TopicAdminSettings topicAdminSettings = TopicAdminSettings
        .newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build();
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
      TopicName topicName = TopicName.of(PROJECT_ID, topicId);
      topicAdminClient.createTopic(topicName);
    }
  }


  private void createSubscription(
      String subscriptionId,
      String topicId,
      TransportChannelProvider channelProvider,
      NoCredentialsProvider credentialsProvider
  ) throws IOException {
    SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings
        .newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build();
    SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings);
    SubscriptionName subscriptionName = SubscriptionName.of(PROJECT_ID, subscriptionId);
    subscriptionAdminClient.createSubscription(
        subscriptionName,
        TopicName.of(PROJECT_ID, topicId),
        PushConfig.getDefaultInstance(),
        10
    );
  }
}