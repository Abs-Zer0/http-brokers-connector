package abs.zer0.http;

import abs.zer0.data.InternalServerError;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

@Controller
public class ErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorHandler.class);

    @Error(global = true)
    public Mono<InternalServerError> globalErrorHandler(Exception error) {
        LOG.error("Internal error", error);
        String message = error.getClass().getSimpleName() + ": " + error.getMessage();

        return Mono.just(new InternalServerError(message));
    }

}
