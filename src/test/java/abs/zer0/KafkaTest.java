package abs.zer0;

import abs.zer0.data.KafkaRecordMetadata;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
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
    HttpClient httpClient;

    @Test
    void sendMessage(TestConsumer consumer) {
        final HttpRequest<String> request = HttpRequest.POST("/kafka/" + TOPIC_NAME_1, MESSAGE_BODY)
                .contentType(MediaType.TEXT_PLAIN_TYPE)
                .accept(MediaType.APPLICATION_JSON_TYPE);
        final HttpResponse<KafkaRecordMetadata> response = httpClient.toBlocking().exchange(request, KafkaRecordMetadata.class);

        assertEquals(HttpStatus.OK, response.status());
        assertNotNull(response.body());
        assertEquals(TOPIC_NAME_1, response.body().topic());

        await()
                .atMost(100, TimeUnit.SECONDS)
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
        public void consumeTopic1(ConsumerRecord<String, String> message) {
            LOG.info("Consumed record");
            consumedTopic1 = message;
        }

        @Topic(TOPIC_NAME_2)
        public void consumeTopic2(ConsumerRecord<String, String> message) {
            consumedTopic2 = message;
        }

    }

}
