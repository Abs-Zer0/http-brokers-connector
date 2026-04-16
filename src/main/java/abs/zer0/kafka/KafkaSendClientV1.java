package abs.zer0.kafka;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.retry.annotation.CircuitBreaker;
import io.micronaut.retry.annotation.Retryable;
import io.micronaut.scheduling.TaskExecutors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Mono;

import java.io.IOException;

@KafkaClient(executor = TaskExecutors.BLOCKING)
public interface KafkaSendClientV1 {

    @CircuitBreaker(includes = IOException.class)
    @Retryable(includes = IOException.class)
    Mono<RecordMetadata> sendMessage(ProducerRecord<String, String> message);

}
