package com.testvt.config;

public class AppConfig {

    // Server
    public static final int    SERVER_PORT              = intEnv("SERVER_PORT", 9090);
    public static final int    SINGLE_THREAD_SERVER_PORT = intEnv("SINGLE_THREAD_SERVER_PORT", 9091);

    // Kafka
    public static final String KAFKA_BOOTSTRAP_SERVERS  = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    public static final String KAFKA_TOPIC              = env("KAFKA_TOPIC", "records");
    public static final String KAFKA_GROUP_ID           = env("KAFKA_GROUP_ID", "batch-worker");
    public static final String KAFKA_PRODUCER_ACKS      = env("KAFKA_PRODUCER_ACKS", "1");
    public static final int    KAFKA_PRODUCER_LINGER_MS = intEnv("KAFKA_PRODUCER_LINGER_MS", 5);
    public static final int    KAFKA_PRODUCER_BATCH_BYTES = intEnv("KAFKA_PRODUCER_BATCH_BYTES", 65536);

    // Batch Worker
    public static final int    BATCH_SIZE               = intEnv("BATCH_SIZE", 500);
    public static final long   BATCH_FLUSH_TIMEOUT_MS   = intEnv("BATCH_FLUSH_TIMEOUT_MS", 200);
    public static final long   KAFKA_POLL_TIMEOUT_MS    = intEnv("KAFKA_POLL_TIMEOUT_MS", 100);

    // Database
    public static final String JDBC_URL = env("JDBC_URL",
            "jdbc:sqlserver://localhost:1433;databaseName=TestVT;encrypt=true;trustServerCertificate=true");
    public static final String DB_USER  = env("DB_USER", "sa");
    public static final String DB_PASS  = env("DB_PASS", "YourPassword123!");

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : def;
    }

    private static int intEnv(String key, int def) {
        try { return Integer.parseInt(System.getenv(key)); } catch (Exception e) { return def; }
    }
}
