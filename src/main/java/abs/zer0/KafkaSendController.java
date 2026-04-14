package abs.zer0;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.http.server.types.files.StreamedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

@Controller("/send")
public class KafkaSendController {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSendController.class);

    @Post(uri = "/{kafkaTopic}/{kafkaKey}", consumes = "text/plain")
    public Mono<String> sendMessageWithKey(
            @PathVariable String kafkaTopic,
            @PathVariable String kafkaKey,
            @Body String body,
            HttpRequest<?> request) {
        
        logRequest(request, body);
        
        return Mono.just("Message sent to topic: " + kafkaTopic + " with key: " + kafkaKey);
    }

    @Post(uri = "/{kafkaTopic}", consumes = "text/plain")
    public Mono<String> sendMessageWithoutKey(
            @PathVariable String kafkaTopic,
            @Body String body,
            HttpRequest<?> request) {
        
        logRequest(request, body);
        
        return Mono.just("Message sent to topic: " + kafkaTopic);
    }

    private void logRequest(HttpRequest<?> request, String body) {
        LOG.info("Real HTTP path: {}", request.getPath());
        LOG.info("Headers: {}", request.getHeaders());
        LOG.info("Body: {}", body);
    }
}
