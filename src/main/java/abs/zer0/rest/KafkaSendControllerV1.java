package abs.zer0.rest;

import abs.zer0.data.InternalServerError;
import abs.zer0.kafka.KafkaSendClientV1;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.version.annotation.Version;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.annotation.Error;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

@Controller(value = "/send", produces = {
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_XML,
        MediaType.APPLICATION_YAML,
        MediaType.TEXT_JSON,
        MediaType.TEXT_XML
}, consumes = {
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_TOML,
        MediaType.APPLICATION_XML,
        MediaType.APPLICATION_YAML,
        MediaType.TEXT_CSV,
        MediaType.TEXT_JSON,
        MediaType.TEXT_PLAIN,
        MediaType.TEXT_XML
})
@Version("1")
public class KafkaSendControllerV1 {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSendControllerV1.class);

    private final KafkaSendClientV1 kafkaSendClient;

    KafkaSendControllerV1(KafkaSendClientV1 kafkaSendClient) {
        this.kafkaSendClient = kafkaSendClient;
    }

    @Post(uri = "/{kafkaTopic}{/kafkaKey}", consumes = "text/plain")
    public Mono<RecordMetadata> sendMessage(
            String kafkaTopic,
            @Nullable String kafkaKey,
            HttpRequest<String> request
    ) {
        ProducerRecord<String, String> message = new ProducerRecord<>(
                kafkaTopic,
                kafkaKey,
                request.getBody().orElse("")
        );
        populateHeaders(message, request);

        return kafkaSendClient.sendMessage(message);
    }

    @Error
    public Mono<InternalServerError> errorHandler(Throwable error) {
        LOG.error("Internal error", error);
        String message = error.getClass().getSimpleName() + ": " + error.getMessage();

        return Mono.just(new InternalServerError(message));
    }

    private void populateHeaders(ProducerRecord<String, String> message, HttpRequest<String> request) {
        request.getHeaders().forEachValue((name, value) -> {
            if (name != null && !name.isEmpty()) {
                message.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
            }
        });
    }

}
