package abs.zer0.http;

import abs.zer0.brokers.KafkaBroker;
import abs.zer0.data.KafkaRecordMetadata;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.version.annotation.Version;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    @ExecuteOn(TaskExecutors.VIRTUAL)
    public HttpResponse<KafkaRecordMetadata> sendMessageV1(
            @PathVariable String topic,
            @PathVariable @Nullable String key,
            HttpRequest<String> request
    ) {
        LOG.debug("[KAFKA] HttpRequest:\nPOST {}\n{}\n{}", request.getPath(), request.getHeaders(), request.getBody());

        final ProducerRecord<String, String> record = buildKafkaRecord(topic, key, request);
        LOG.debug("[KAFKA] ProducerRecord:\n{}", record);

        final RecordMetadata recordMetadata = kafka.sendMessageV1(record);
        LOG.debug("[KAFKA] RecordMetadata:\n{}", recordMetadata);

        HttpResponse<KafkaRecordMetadata> response = HttpResponse.ok(new KafkaRecordMetadata(
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                Instant.ofEpochMilli(recordMetadata.timestamp())
        ));
        LOG.debug("[KAFKA] HttpResponse:\n{} {}\n{}\n{}", response.code(), response.reason(), response.getHeaders(), response.body());
        LOG.info("[KAFKA] Successful send to {}", topic);

        return response;
    }

    private ProducerRecord<String, String> buildKafkaRecord(String topic, String key, HttpRequest<String> request) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                key,
                request.getBody().orElse(null)
        );

        request.getHeaders().forEachValue((name, value) -> {
            if (name != null && !name.isEmpty()) {
                record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
            }
        });

        return record;
    }

}
