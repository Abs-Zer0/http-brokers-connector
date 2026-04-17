package abs.zer0;


import abs.zer0.kafka.KafkaSendClientV1;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
class HttpToKafkaBridgeTest {

    @Inject
    @Client("/")
    HttpClient httpClient;

    private final AtomicReference<KafkaSendClientV1> mockRef = new AtomicReference<>();

    @MockBean(KafkaSendClientV1.class)
    KafkaSendClientV1 mockKafkaSendClient() {
        KafkaSendClientV1 mock = message -> {
            throw new UnsupportedOperationException("Not implemented");
        };
        mockRef.set(mock);
        return mock;
    }

    @Test
    void testSuccessfulSendMessageWithoutKey() {
        String kafkaTopic = "test-topic";
        String body = "test message";

        RecordMetadata expectedMetadata = createMockRecordMetadata(kafkaTopic, 0L, 0, 0);
        
        KafkaSendClientV1 mockClient = new KafkaSendClientV1() {
            @Override
            public Mono<RecordMetadata> sendMessage(ProducerRecord<String, String> message) {
                return Mono.just(expectedMetadata);
            }
        };
        mockRef.set(mockClient);

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpResponse<RecordMetadata> response = httpClient.toBlocking().exchange(request, RecordMetadata.class);

        assertEquals(HttpStatus.OK, response.getStatus());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isPresent());
        assertEquals(kafkaTopic, response.getBody().get().topic());
    }

    @Test
    void testSuccessfulSendMessageWithKey() {
        String kafkaTopic = "test-topic-with-key";
        String kafkaKey = "test-key";
        String body = "test message with key";

        RecordMetadata expectedMetadata = createMockRecordMetadata(kafkaTopic, 0L, 0, 0);
        
        KafkaSendClientV1 mockClient = new KafkaSendClientV1() {
            @Override
            public Mono<RecordMetadata> sendMessage(ProducerRecord<String, String> message) {
                return Mono.just(expectedMetadata);
            }
        };
        mockRef.set(mockClient);

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic + "/" + kafkaKey, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpResponse<RecordMetadata> response = httpClient.toBlocking().exchange(request, RecordMetadata.class);

        assertEquals(HttpStatus.OK, response.getStatus());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isPresent());
        assertEquals(kafkaTopic, response.getBody().get().topic());
    }

    @Test
    void testAllIncomingHeadersArePropagatedToTopic() {
        String kafkaTopic = "test-topic-headers";
        String body = "test message with headers";
        String headerName1 = "X-Custom-Header";
        String headerValue1 = "custom-value";
        String headerName2 = "X-Another-Header";
        String headerValue2 = "another-value";

        RecordMetadata expectedMetadata = createMockRecordMetadata(kafkaTopic, 0L, 0, 0);
        
        KafkaSendClientV1 mockClient = new KafkaSendClientV1() {
            @Override
            public Mono<RecordMetadata> sendMessage(ProducerRecord<String, String> message) {
                Headers headers = message.headers();
                
                assertNotNull(headers.lastHeader(headerName1));
                assertEquals(headerValue1, new String(headers.lastHeader(headerName1).value(), StandardCharsets.UTF_8));
                
                assertNotNull(headers.lastHeader(headerName2));
                assertEquals(headerValue2, new String(headers.lastHeader(headerName2).value(), StandardCharsets.UTF_8));
                
                return Mono.just(expectedMetadata);
            }
        };
        mockRef.set(mockClient);

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE)
                .header(headerName1, headerValue1)
                .header(headerName2, headerValue2);

        HttpResponse<RecordMetadata> response = httpClient.toBlocking().exchange(request, RecordMetadata.class);

        assertEquals(HttpStatus.OK, response.getStatus());
        assertNotNull(response.getBody());
    }

    @Test
    void testCorrectMetadataReturnedOnSuccess() {
        String kafkaTopic = "test-topic-metadata";
        String kafkaKey = "metadata-key";
        String body = "test message for metadata";
        long expectedOffset = 12345L;
        int expectedPartition = 5;

        RecordMetadata expectedMetadata = createMockRecordMetadata(kafkaTopic, expectedOffset, expectedPartition, 0);
        
        KafkaSendClientV1 mockClient = new KafkaSendClientV1() {
            @Override
            public Mono<RecordMetadata> sendMessage(ProducerRecord<String, String> message) {
                return Mono.just(expectedMetadata);
            }
        };
        mockRef.set(mockClient);

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic + "/" + kafkaKey, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpResponse<RecordMetadata> response = httpClient.toBlocking().exchange(request, RecordMetadata.class);

        assertEquals(HttpStatus.OK, response.getStatus());
        assertTrue(response.getBody().isPresent());
        RecordMetadata metadata = response.getBody().get();
        assertEquals(kafkaTopic, metadata.topic());
        assertEquals(expectedOffset, metadata.offset());
        assertEquals(expectedPartition, metadata.partition());
    }

    @Test
    void testErrorResponseReturnsCorrectFormat() {
        String kafkaTopic = "test-topic-error";
        String body = "test message causing error";
        String errorMessage = "Kafka connection failed";

        RuntimeException exception = new RuntimeException(errorMessage);
        
        KafkaSendClientV1 mockClient = new KafkaSendClientV1() {
            @Override
            public Mono<RecordMetadata> sendMessage(ProducerRecord<String, String> message) {
                return Mono.error(exception);
            }
        };
        mockRef.set(mockClient);

        MutableHttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpClientResponseException clientException = assertThrows(
                HttpClientResponseException.class,
                () -> httpClient.toBlocking().exchange(request, RecordMetadata.class)
        );

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, clientException.getStatus());
        String responseBody = clientException.getResponse().getBody(String.class).orElse("");
        assertTrue(responseBody.contains("RuntimeException"));
        assertTrue(responseBody.contains(errorMessage));
    }

    private RecordMetadata createMockRecordMetadata(String topic, long offset, int partition, int hashCode) {
        return new RecordMetadata(
                new org.apache.kafka.common.TopicPartition(topic, partition),
                offset,
                System.currentTimeMillis(),
                -1L,
                -1L,
                -1,
                -1,
                new RecordHeaders()
        );
    }
}
