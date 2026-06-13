package com.testvt.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.testvt.config.AppConfig;
import com.testvt.db.DatabaseManager;
import com.testvt.model.Record;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.common.KafkaException;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes records from Kafka, accumulates them into a batch,
 * then flushes to DB with a single bulk INSERT.
 *
 * Flush when EITHER condition is met (OR logic per design doc §2.3):
 *   (1) batch.size() >= BATCH_SIZE
 *   (2) elapsed since last flush >= BATCH_FLUSH_TIMEOUT_MS
 */
public class KafkaBatchWorker implements Runnable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DatabaseManager db;
    private volatile boolean running = true;

    private final AtomicLong totalInserted  = new AtomicLong(0);
    private final AtomicLong totalBatches   = new AtomicLong(0);
    private final AtomicLong totalFlushTime = new AtomicLong(0);
    private final AtomicLong totalErrors    = new AtomicLong(0);

    public KafkaBatchWorker(DatabaseManager db) {
        this.db = db;
    }

    @Override
    public void run() {
        System.out.printf("[KafkaBatchWorker] Started. topic=%s group=%s batch_size=%d flush_timeout=%dms%n",
                AppConfig.KAFKA_TOPIC, AppConfig.KAFKA_GROUP_ID,
                AppConfig.BATCH_SIZE, AppConfig.BATCH_FLUSH_TIMEOUT_MS);

        try (KafkaConsumer<String, String> consumer = buildConsumer()) {
            consumer.subscribe(List.of(AppConfig.KAFKA_TOPIC));

            List<Record> batch = new ArrayList<>(AppConfig.BATCH_SIZE);
            long lastFlush = System.currentTimeMillis();

            while (running) {
                ConsumerRecords<String, String> polled =
                        consumer.poll(Duration.ofMillis(AppConfig.KAFKA_POLL_TIMEOUT_MS));

                for (ConsumerRecord<String, String> cr : polled) {
                    Record r = parse(cr.value());
                    if (r != null) {
                        batch.add(r);
                        // Flush mid-poll if we hit the size limit — prevents exceeding
                        // SQL Server's 2100-parameter cap (4 params × 500 records = 2000)
                        if (batch.size() >= AppConfig.BATCH_SIZE) {
                            flush(consumer, batch);
                            lastFlush = System.currentTimeMillis();
                            batch = new ArrayList<>(AppConfig.BATCH_SIZE);
                        }
                    }
                }

                long now = System.currentTimeMillis();
                boolean timeoutReached = (now - lastFlush) >= AppConfig.BATCH_FLUSH_TIMEOUT_MS;

                if (!batch.isEmpty() && timeoutReached) {
                    flush(consumer, batch);
                    lastFlush = System.currentTimeMillis();
                    batch = new ArrayList<>(AppConfig.BATCH_SIZE);
                }
            }

            // Drain remaining records before exit
            if (!batch.isEmpty()) {
                flush(consumer, batch);
            }
        }

        System.out.printf("[KafkaBatchWorker] Done. total_inserted=%d batches=%d avg_flush=%.1fms errors=%d%n",
                totalInserted.get(), totalBatches.get(),
                totalBatches.get() == 0 ? 0.0 : (double) totalFlushTime.get() / totalBatches.get(),
                totalErrors.get());
    }

    private void flush(KafkaConsumer<String, String> consumer, List<Record> batch) {
        long t0 = System.currentTimeMillis();
        try {
            db.insertBatch(batch);
            consumer.commitSync();   // commit only after successful DB write

            long elapsed = System.currentTimeMillis() - t0;
            totalInserted.addAndGet(batch.size());
            totalBatches.incrementAndGet();
            totalFlushTime.addAndGet(elapsed);

            System.out.printf("[KafkaBatchWorker] Flushed %d records in %dms | total=%d%n",
                    batch.size(), elapsed, totalInserted.get());
        } catch (SQLException e) {
            System.err.printf("[KafkaBatchWorker] Batch insert failed (%d records), falling back to single inserts: %s%n",
                    batch.size(), e.getMessage());
            fallbackSingleInsert(batch);
        } catch (KafkaException e) {
            // DB write succeeded but commit failed — record counted but offset may re-deliver
            totalErrors.incrementAndGet();
            System.err.printf("[KafkaBatchWorker] Kafka commit error: %s | errors=%d%n",
                    e.getMessage(), totalErrors.get());
        }
    }

    private void fallbackSingleInsert(List<Record> batch) {
        for (Record r : batch) {
            try {
                db.insertSingle(r);
                totalInserted.incrementAndGet();
            } catch (SQLException ex) {
                totalErrors.incrementAndGet();
                System.err.printf("[KafkaBatchWorker] Failed record id=%s clientId=%s ts=%d: %s%n",
                        r.getId(), r.getClientId(), r.getTimestamp(), ex.getMessage());
            }
        }
    }

    private Record parse(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            return new Record(
                    node.get("id").asText(),
                    node.get("payload").asText(),
                    node.get("timestamp").asLong(),
                    node.get("clientId").asText(),
                    node.has("threadId") ? node.get("threadId").asLong() : 0L
            );
        } catch (Exception e) {
            System.err.println("[KafkaBatchWorker] Parse error: " + e.getMessage());
            return null;
        }
    }

    private KafkaConsumer<String, String> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  AppConfig.KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           AppConfig.KAFKA_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Manual commit — we commit only after successful DB insert
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        // Pre-fetch up to BATCH_SIZE records per poll
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   String.valueOf(AppConfig.BATCH_SIZE));
        return new KafkaConsumer<>(props);
    }

    public void stop() { this.running = false; }

    public long getTotalInserted() { return totalInserted.get(); }
    public long getTotalBatches()  { return totalBatches.get(); }
    public long getTotalErrors()   { return totalErrors.get(); }
}
