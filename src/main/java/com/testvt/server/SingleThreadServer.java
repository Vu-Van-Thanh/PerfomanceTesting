package com.testvt.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.testvt.config.AppConfig;
import com.testvt.db.DatabaseManager;
import com.testvt.model.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Single-threaded TCP server.
 *
 * Architecture:
 *   Client → TCP → [accept → handleClient → next] (sequential, no thread pool)
 *                                     ↓
 *                             DB.insertSingle() → SQL Server (direct, synchronous)
 *
 * Contrast with Server.java (multithread → Kafka → batch insert).
 */
public class SingleThreadServer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DatabaseManager db;
    private volatile boolean running = true;

    private long totalProcessed = 0;
    private long startTime;

    public SingleThreadServer() throws Exception {
        this.db = new DatabaseManager(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASS);
        db.createTableIfNotExists();
    }

    public void start() throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        startTime = System.currentTimeMillis();

        System.out.printf("[SingleThreadServer] Listening on port %d | mode=sequential | storage=direct-db%n",
                AppConfig.SINGLE_THREAD_SERVER_PORT);

        try (ServerSocket serverSocket = new ServerSocket(AppConfig.SINGLE_THREAD_SERVER_PORT)) {
            while (running) {
                try {
                    Socket conn = serverSocket.accept();
                    handleClient(conn);
                } catch (IOException e) {
                    if (running) System.err.println("[SingleThreadServer] Accept error: " + e.getMessage());
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        try (BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter    out = new PrintWriter(socket.getOutputStream(), true)) {

            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                try {
                    Record record = parse(line);
                    db.insertSingle(record);
                    out.println("OK");
                    totalProcessed++;
                    if (totalProcessed % 100 == 0) {
                        long elapsed = System.currentTimeMillis() - startTime;
                        System.out.printf("[SingleThreadServer] processed=%d | throughput=%.1f rec/s%n",
                                totalProcessed, totalProcessed * 1000.0 / elapsed);
                    }
                } catch (Exception e) {
                    out.println("ERROR:WRITE_FAILED");
                    System.err.println("[SingleThreadServer] Write error: " + e.getMessage());
                }
            }
        } catch (IOException ignored) {
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private Record parse(String json) throws Exception {
        JsonNode node = MAPPER.readTree(json);
        return new Record(
                node.get("id").asText(),
                node.get("payload").asText(),
                node.get("timestamp").asLong(),
                node.get("clientId").asText(),
                Thread.currentThread().getId()
        );
    }

    private void shutdown() {
        System.out.println("[SingleThreadServer] Shutting down...");
        running = false;
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.printf("[SingleThreadServer] Total processed=%d | elapsed=%d ms | throughput=%.1f rec/s%n",
                totalProcessed, elapsed, totalProcessed * 1000.0 / Math.max(1, elapsed));
        db.close();
    }

    public static void main(String[] args) throws Exception {
        new SingleThreadServer().start();
    }
}
