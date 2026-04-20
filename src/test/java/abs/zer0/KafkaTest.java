package abs.zer0;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
public class KafkaTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTest.class);

    private static final String TOPIC_NAME_1 = "test-topic.1";
    private static final String TOPIC_NAME_2 = "another-topic.2";
    private static final String MESSAGE_KEY = "msg-key";
    private static final String MESSAGE_BODY = "Text text text ...";

    @Test
    void sendMessage(RequestSpecification spec, TestConsumer consumer) {
        spec
                .given()
                    .body(MESSAGE_BODY)
                    .contentType(ContentType.TEXT)
                .when()
                    .post("/kafka/" + TOPIC_NAME_1)
                .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("topic", equalTo(TOPIC_NAME_1))
        ;

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
