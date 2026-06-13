package com.testvt.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.testvt.config.AppConfig;
import com.testvt.model.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class RecordProducer implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaProducer<String, String> producer;
    private final String topic;

    private final AtomicLong sentCount  = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public RecordProducer() {
        this.topic = AppConfig.KAFKA_TOPIC;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  AppConfig.KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,               AppConfig.KAFKA_PRODUCER_ACKS);
        props.put(ProducerConfig.LINGER_MS_CONFIG,          AppConfig.KAFKA_PRODUCER_LINGER_MS);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,         AppConfig.KAFKA_PRODUCER_BATCH_BYTES);
        // Idempotent delivery within a session
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Async send — returns immediately, errors are logged via callback.
     * Returns false only if JSON serialization fails (should never happen).
     */
    public boolean send(Record record) {
        try {
            String json = MAPPER.writeValueAsString(new RecordDto(
                    record.getId(), record.getPayload(), record.getTimestamp(), record.getClientId()));

            producer.send(
                new ProducerRecord<>(topic, record.getId(), json),
                (metadata, ex) -> {
                    if (ex != null) {
                        errorCount.incrementAndGet();
                        System.err.println("[Producer] Send error: " + ex.getMessage());
                    } else {
                        sentCount.incrementAndGet();
                    }
                }
            );
            return true;
        } catch (Exception e) {
            errorCount.incrementAndGet();
            return false;
        }
    }

    public long getSentCount()  { return sentCount.get(); }
    public long getErrorCount() { return errorCount.get(); }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    record RecordDto(String id, String payload, long timestamp, String clientId) {}
}
