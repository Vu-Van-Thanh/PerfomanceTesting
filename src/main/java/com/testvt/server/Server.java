package com.testvt.server;

import com.testvt.config.AppConfig;
import com.testvt.db.DatabaseManager;
import com.testvt.kafka.RecordProducer;
import com.testvt.worker.KafkaBatchWorker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Multi-threaded TCP server.
 *
 * Architecture:
 *   Client → TCP → ThreadPool (ClientHandler) → KafkaProducer → [Kafka topic]
 *                                                                      ↓
 *                                                            KafkaBatchWorker → DB
 */
public class Server {

    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    private final ExecutorService   workerPool;
    private final RecordProducer    producer;
    private final DatabaseManager   db;
    private final KafkaBatchWorker  batchWorker;
    private final Thread            batchWorkerThread;
    private volatile boolean        running = true;

    public Server() throws Exception {
        this.workerPool       = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        this.producer         = new RecordProducer();
        this.db               = new DatabaseManager(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASS);
        this.batchWorker      = new KafkaBatchWorker(db);
        this.batchWorkerThread = new Thread(batchWorker, "kafka-batch-worker");

        db.createTableIfNotExists();
    }

    public void start() throws IOException {
        batchWorkerThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        System.out.printf("[Server] Listening on port %d | threads=%d | kafka=%s topic=%s%n",
                AppConfig.SERVER_PORT, THREAD_POOL_SIZE,
                AppConfig.KAFKA_BOOTSTRAP_SERVERS, AppConfig.KAFKA_TOPIC);

        try (ServerSocket serverSocket = new ServerSocket(AppConfig.SERVER_PORT)) {
            while (running) {
                Socket conn = serverSocket.accept();
                workerPool.submit(new ClientHandler(conn, producer));
            }
        }
    }

    private void shutdown() {
        System.out.println("[Server] Shutting down...");
        running = false;

        workerPool.shutdown();
        try { workerPool.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}

        producer.close();

        batchWorker.stop();
        try { batchWorkerThread.join(10_000); } catch (InterruptedException ignored) {}

        db.close();
        System.out.println("[Server] Shutdown complete.");
    }

    public static void main(String[] args) throws Exception {
        new Server().start();
    }
}
