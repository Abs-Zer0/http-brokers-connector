package abs.zer0.data;

import io.micronaut.serde.annotation.Serdeable;

import java.time.Instant;

@Serdeable
public record KafkaRecordMetadata(
        String topic,
        int partition,
        long offset,
        Instant timestamp
) {
}
