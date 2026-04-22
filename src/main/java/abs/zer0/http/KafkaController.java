package abs.zer0.http;

import abs.zer0.brokers.KafkaBroker;
import abs.zer0.data.KafkaRecordMetadata;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.version.annotation.Version;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Controller(value = "/kafka", produces = {
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
public class KafkaController {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaController.class);

    private final KafkaBroker kafka;

    KafkaController(KafkaBroker kafka) {
        this.kafka = kafka;
    }

    @Post("{topic}{/key}")
    @Version("1")
    public Mono<KafkaRecordMetadata> sendMessageV1(
            @PathVariable  String topic,
            @PathVariable @Nullable String key,
            HttpRequest<?> request
    ) {
        LOG.info("HttpRequest: {}", request.getBody());
        ProducerRecord<String, String> message = new ProducerRecord<>(
                topic,
                key,
                request.getBody(String.class).orElse(null)
        );
        populateHeaders(message, request);
        LOG.info("KAFKA ProducerRecord: {}", message);

        return kafka.sendMessageV1(message)
                .map(recordMetadata -> new KafkaRecordMetadata(
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        Instant.ofEpochMilli(recordMetadata.timestamp())
                ));
    }

    private void populateHeaders(ProducerRecord<String, String> message, HttpRequest<?> request) {
        request.getHeaders().forEachValue((name, value) -> {
            if (name != null && !name.isEmpty()) {
                message.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
            }
        });
    }

}
