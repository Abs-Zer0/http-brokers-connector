package abs.zer0.brokers;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.retry.annotation.CircuitBreaker;
import io.micronaut.retry.annotation.Retryable;
import io.micronaut.scheduling.TaskExecutors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;

@KafkaClient(executor = TaskExecutors.VIRTUAL)
public interface KafkaBroker {

    @CircuitBreaker(includes = IOException.class)
    @Retryable(includes = IOException.class)
    RecordMetadata sendMessageV1(ProducerRecord<String, String> record);

}
