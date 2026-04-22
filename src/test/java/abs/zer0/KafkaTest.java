package abs.zer0;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.*;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
public class KafkaTest {

    private static final String TOPIC_A = "test-topic.a";
    private static final String TOPIC_B = "another-topic.b";

    private static final Map<String, BlockingQueue<ConsumerRecord<String, String>>> CONSUMED_MESSAGES = Map.of(
            TOPIC_A, new LinkedBlockingQueue<>(),
            TOPIC_B, new LinkedBlockingQueue<>()
    );

    @Test
    void v1TopicA(RequestSpecification spec) {
        final String body = "Text message without key and headers";

        spec
                .given()
                    .log().all()
                    .body(body)
                    .contentType(ContentType.TEXT)
                .when()
                    .post("/kafka/v1/" + TOPIC_A)
                .then()
                    .log().all()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("topic", equalTo(TOPIC_A))
        ;

        await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    final ConsumerRecord<String, String> consumed = CONSUMED_MESSAGES.get(TOPIC_A).poll(1, TimeUnit.SECONDS);

                    assertNotNull(consumed);
                    assertEquals(TOPIC_A, consumed.topic());
                    assertNull(consumed.key());
                    assertEquals(body, consumed.value());
                })
        ;
    }

    @Test
    void v2TopicB(RequestSpecification spec) {
        final String body = "Text message without key and headers";

        spec
                .given()
                    .log().all()
                    .body(body)
                    .contentType(ContentType.TEXT)
                .when()
                    .post("/kafka/v1/" + TOPIC_B)
                .then()
                    .log().all()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("topic", equalTo(TOPIC_B))
        ;

        await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    final ConsumerRecord<String, String> consumed = CONSUMED_MESSAGES.get(TOPIC_B).poll(1, TimeUnit.SECONDS);

                    assertNotNull(consumed);
                    assertEquals(TOPIC_B, consumed.topic());
                    assertNull(consumed.key());
                    assertEquals(body, consumed.value());
                })
        ;
    }


    @KafkaListener
    static class TestConsumer {

        @Topic(TOPIC_A)
        public void consumeTopicA(ConsumerRecord<String, String> record) {
            CONSUMED_MESSAGES.get(TOPIC_A).add(record);
        }

        @Topic(TOPIC_B)
        public void consumeTopicB(ConsumerRecord<String, String> record) {
            CONSUMED_MESSAGES.get(TOPIC_B).add(record);
        }

    }

}
