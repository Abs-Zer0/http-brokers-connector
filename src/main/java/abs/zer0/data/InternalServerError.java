package abs.zer0.data;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record InternalServerError(String message) {
}
