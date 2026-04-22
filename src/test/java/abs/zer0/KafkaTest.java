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

    @Test
    void sendMessageTopic1(RequestSpecification spec, Consumer1 consumer) {
        final String body = "Text message without key and headers";

        spec
                .given()
                    .log().all()
                    .body(body)
                    .contentType(ContentType.TEXT)
                    .header("Content-Length", body.getBytes("UTF-8").length)
                .when()
                    .post("/kafka/" + TOPIC_NAME_1)
                .then()
                    .log().all()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("topic", equalTo(TOPIC_NAME_1))
        ;

        await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertNotNull(consumer.consumed);
                    assertEquals(TOPIC_NAME_1, consumer.consumed.topic());
                    assertNull(consumer.consumed.key());
                    assertEquals(body, consumer.consumed.value());
                })
        ;

        consumer.consumed = null;
    }

    @Test
    void sendMessageTopic2(RequestSpecification spec, Consumer2 consumer) {
        final String body = "Text message without key and headers";

        spec
                .given()
                    .log().all()
                    .body(body)
                    .contentType(ContentType.TEXT)
                .when()
                    .post("/kafka/" + TOPIC_NAME_2)
                .then()
                    .log().all()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("topic", equalTo(TOPIC_NAME_2))
        ;

        await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertNotNull(consumer.consumed);
                    assertEquals(TOPIC_NAME_2, consumer.consumed.topic());
                    assertNull(consumer.consumed.key());
                    assertEquals(body, consumer.consumed.value());
                })
        ;

        consumer.consumed = null;
    }


    @KafkaListener
    static class Consumer1 {

        ConsumerRecord<String, String> consumed;

        @Topic(TOPIC_NAME_1)
        public void consume(ConsumerRecord<String, String> message) {
            consumed = message;
            LOG.info("Consumed: {}", consumed);
        }

    }

    @KafkaListener
    static class Consumer2 {

        ConsumerRecord<String, String> consumed;

        @Topic(TOPIC_NAME_2)
        public void consume(ConsumerRecord<String, String> message) {
            consumed = message;
            LOG.info("Consumed: {}", consumed);
        }

    }

}
