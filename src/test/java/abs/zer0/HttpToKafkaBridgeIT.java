package abs.zer0;


import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@MicronautTest
class HttpToKafkaBridgeIT {

    @Container
    static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );

    @Inject
    @Client("/")
    HttpClient httpClient;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        consumer = new KafkaConsumer<>(props);
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void testSuccessfulSendMessageWithoutKey() {
        String kafkaTopic = "test-topic-no-key";
        String body = "test message without key";

        consumer.subscribe(Collections.singletonList(kafkaTopic));

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpResponse<?> response = httpClient.toBlocking().exchange(request);

        assertEquals(HttpStatus.OK, response.getStatus());

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                assertFalse(records.isEmpty(), "Expected at least one record in topic");
                
                ConsumerRecord<String, String> record = records.iterator().next();
                assertEquals(body, record.value());
                assertNull(record.key());
            });
    }

    @Test
    void testSuccessfulSendMessageWithKey() {
        String kafkaTopic = "test-topic-with-key";
        String kafkaKey = "test-key-value";
        String body = "test message with key";

        consumer.subscribe(Collections.singletonList(kafkaTopic));

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic + "/" + kafkaKey, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpResponse<?> response = httpClient.toBlocking().exchange(request);

        assertEquals(HttpStatus.OK, response.getStatus());

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                assertFalse(records.isEmpty(), "Expected at least one record in topic");
                
                ConsumerRecord<String, String> record = records.iterator().next();
                assertEquals(body, record.value());
                assertEquals(kafkaKey, record.key());
            });
    }

    @Test
    void testAllIncomingHeadersArePropagatedToTopic() {
        String kafkaTopic = "test-topic-headers";
        String body = "test message with headers";
        String headerName1 = "X-Custom-Header";
        String headerValue1 = "custom-value";
        String headerName2 = "X-Another-Header";
        String headerValue2 = "another-value";

        consumer.subscribe(Collections.singletonList(kafkaTopic));

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE)
                .header(headerName1, headerValue1)
                .header(headerName2, headerValue2);

        HttpResponse<?> response = httpClient.toBlocking().exchange(request);

        assertEquals(HttpStatus.OK, response.getStatus());

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                assertFalse(records.isEmpty(), "Expected at least one record in topic");
                
                ConsumerRecord<String, String> record = records.iterator().next();
                assertEquals(body, record.value());
                
                Map<String, String> headers = toMap(record.headers());
                assertTrue(headers.containsKey(headerName1), "Header " + headerName1 + " should be present");
                assertEquals(headerValue1, headers.get(headerName1));
                
                assertTrue(headers.containsKey(headerName2), "Header " + headerName2 + " should be present");
                assertEquals(headerValue2, headers.get(headerName2));
            });
    }

    @Test
    void testCorrectMetadataReturnedOnSuccess() {
        String kafkaTopic = "test-topic-metadata";
        String kafkaKey = "metadata-key";
        String body = "test message for metadata";

        consumer.subscribe(Collections.singletonList(kafkaTopic));

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic + "/" + kafkaKey, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpResponse<?> response = httpClient.toBlocking().exchange(request);

        assertEquals(HttpStatus.OK, response.getStatus());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isPresent());
    }

    @Test
    void testErrorResponseReturnsCorrectFormat() {
        String kafkaTopic = "nonexistent-topic-error";
        String body = "test message causing error";

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpClientResponseException clientException = assertThrows(
                HttpClientResponseException.class,
                () -> httpClient.toBlocking().exchange(request)
        );

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, clientException.getStatus());
        String responseBody = clientException.getResponse().getBody(String.class).orElse("");
        assertTrue(responseBody.contains("message"), "Response should contain 'message' field");
    }

    private Map<String, String> toMap(org.apache.kafka.common.header.Headers headers) {
        Map<String, String> headerMap = new java.util.HashMap<>();
        headers.forEach(header -> 
            headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8))
        );
        return headerMap;
    }
}
