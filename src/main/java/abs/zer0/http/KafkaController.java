package abs.zer0.http;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.version.annotation.Version;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Mono;

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

    @Post("{topic}{/key}")
    @Version("1")
    public Mono<RecordMetadata> sendMessageV1(
            String topic,
            @Nullable String key,
            HttpRequest<String> request
    ) {
        return Mono.empty();
    }

}
