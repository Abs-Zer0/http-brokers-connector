package abs.zer0;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

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
public class RestController {

    private static final Logger LOG = LoggerFactory.getLogger(RestController.class);

    @Post(uri = "/{kafkaTopic}", consumes = "text/plain")
    public Mono<HttpResponse<String>> sendMessage(
            @PathVariable String kafkaTopic,
            HttpRequest<String> request
    ) {
        logRequest(request);

        return Mono.just(HttpResponse.ok("Message sent to topic: " + kafkaTopic));
    }

    @Post(uri = "/{kafkaTopic}/{kafkaKey}", consumes = "text/plain")
    public Mono<HttpResponse<String>> sendMessageWithKey(
            @PathVariable("kafkaTopic") String kafkaTopic,
            @PathVariable("kafkaKey") String kafkaKey,
            HttpRequest<String> request
    ) {
        logRequest(request);

        return Mono.just(HttpResponse.ok("Message sent to topic: " + kafkaTopic + " with key: " + kafkaKey));
    }

    private void logRequest(HttpRequest<String> request) {
        LOG.info("Real HTTP path: {}", request.getPath());
        LOG.info("Headers: {}", request.getHeaders());
        LOG.info("Body: {}", request.getBody());
    }

}
