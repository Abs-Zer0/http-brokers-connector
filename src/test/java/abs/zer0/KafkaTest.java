package abs.zer0;

import abs.zer0.data.InternalServerError;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
public class KafkaTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTest.class);

    private static final String TOPIC_NAME_1 = "test-topic.1";
    private static final String TOPIC_NAME_2 = "another-topic.2";
    private static final String MESSAGE_KEY = "msg-key";
    private static final String MESSAGE_BODY = "Text text text ...";

    @Inject
    @Client("/kafka")
    HttpClient httpClient;

    @Test
    void sendMessage(TestConsumer consumer) {
        final HttpRequest<String> request = HttpRequest.POST(TOPIC_NAME_1, MESSAGE_BODY)
                .contentType(MediaType.TEXT_PLAIN_TYPE);
        try {
            final HttpResponse<String> response = httpClient.toBlocking().exchange(request, String.class);

            assertEquals(HttpStatus.OK, response.status());
            assertNotNull(response.body());
            assertTrue(response.body().contains(TOPIC_NAME_1));
        } catch (HttpClientResponseException hcre) {
            LOG.error("Error", hcre);
        }

        await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertNotNull(consumer.consumedTopic1);
                    assertNull(consumer.consumedTopic1.key());
                    assertEquals(MESSAGE_BODY, consumer.consumedTopic1.value());
                });
        consumer.consumedTopic1 = null;
    }

    @KafkaListener
    static class TestConsumer {

        ConsumerRecord<String, String> consumedTopic1;
        ConsumerRecord<String, String> consumedTopic2;

        @Topic(TOPIC_NAME_1)
        void consumeTopic1(ConsumerRecord<String, String> message) {
            consumedTopic1 = message;
        }

        @Topic(TOPIC_NAME_2)
        void consumeTopic2(ConsumerRecord<String, String> message) {
            consumedTopic2 = message;
        }

    }

}
