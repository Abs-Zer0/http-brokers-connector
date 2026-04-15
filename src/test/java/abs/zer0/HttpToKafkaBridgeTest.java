package abs.zer0;


import io.micronaut.runtime.EmbeddedApplication;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import jakarta.inject.Inject;
import io.micronaut.http.client.HttpClient;

@MicronautTest
class HttpToKafkaBridgeTest {

    @Inject
    EmbeddedApplication<?> application;

    @Inject
    @Client("/")
    HttpClient httpClient;

    @Test
    void testItWorks() {
        Assertions.assertTrue(application.isRunning());
        Assertions.assertTrue(httpClient != null);
    }

    @Test
    void testSendMessageWithKey() {
        String kafkaTopic = "test-topic";
        String kafkaKey = "test-key";
        String body = "test message";

        HttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic + "/" + kafkaKey, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpResponse<String> response = httpClient.toBlocking().exchange(request, String.class);

        Assertions.assertEquals(HttpStatus.OK, response.getStatus());
        Assertions.assertTrue(response.getBody().isPresent());
        Assertions.assertTrue(response.getBody().get().contains(kafkaTopic));
        Assertions.assertTrue(response.getBody().get().contains(kafkaKey));
    }

    @Test
    void testSendMessageWithoutKey() {
        String kafkaTopic = "test-topic-no-key";
        String body = "test message without key";

        HttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpResponse<String> response = httpClient.toBlocking().exchange(request, String.class);

        Assertions.assertEquals(HttpStatus.OK, response.getStatus());
        Assertions.assertTrue(response.getBody().isPresent());
        Assertions.assertTrue(response.getBody().get().contains(kafkaTopic));
    }

    @Test
    void testSendMessageWithKeyEmptyBody() {
        String kafkaTopic = "test-topic-empty";
        String kafkaKey = "empty-key";
        String body = "";

        HttpRequest<String> request = HttpRequest.POST("/send/" + kafkaTopic + "/" + kafkaKey, body)
                .contentType(io.micronaut.http.MediaType.TEXT_PLAIN_TYPE);

        HttpResponse<String> response = httpClient.toBlocking().exchange(request, String.class);

        Assertions.assertEquals(HttpStatus.OK, response.getStatus());
        Assertions.assertTrue(response.getBody().isPresent());
    }

}
