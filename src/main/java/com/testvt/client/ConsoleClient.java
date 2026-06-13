package com.testvt.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.testvt.config.AppConfig;
import com.testvt.db.DatabaseManager;
import com.testvt.model.Record;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Interactive console client for performance testing.
 *
 * Allows choosing between the multithread server (Kafka pipeline) and
 * the single-thread server (direct DB write), then sends messages
 * concurrently with a live progress bar.
 */
public class ConsoleClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String MULTITHREAD_HOST  = System.getenv().getOrDefault("MULTITHREAD_HOST",  "localhost");
    private static final String SINGLE_THREAD_HOST = System.getenv().getOrDefault("SINGLE_THREAD_HOST", "localhost");

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println();
            System.out.println("=== Performance Testing Console ===");
            System.out.printf("[1] Multithread Server   (%s:%d)%n", MULTITHREAD_HOST,  AppConfig.SERVER_PORT);
            System.out.printf("[2] Single Thread Server (%s:%d)%n", SINGLE_THREAD_HOST, AppConfig.SINGLE_THREAD_SERVER_PORT);
            System.out.println("[3] Xóa toàn bộ dữ liệu trong DB");
            System.out.println("[q] Thoát");
            System.out.print("> ");

            String choice = scanner.nextLine().trim();

            if ("q".equalsIgnoreCase(choice)) {
                System.out.println("Thoát.");
                break;
            }

            if ("3".equals(choice)) {
                clearDatabase(scanner);
                continue;
            }

            int port;
            String host;
            String serverName;
            if ("1".equals(choice)) {
                port = AppConfig.SERVER_PORT;
                serverName = "Multithread";
                host = MULTITHREAD_HOST;
            } else if ("2".equals(choice)) {
                port = AppConfig.SINGLE_THREAD_SERVER_PORT;
                serverName = "Single Thread";
                host = SINGLE_THREAD_HOST;
            } else {
                System.out.println("Lựa chọn không hợp lệ. Nhập 1, 2, 3 hoặc q.");
                continue;
            }

            boolean multithread = "1".equals(choice);
            int numClients = readInt(scanner, "Số concurrent clients", 50);
            int recordsPerClient = readInt(scanner, "Số records/client", 200);

            runSimulation(host, port, serverName, multithread, numClients, recordsPerClient);

            System.out.println("\nNhấn Enter để tiếp tục...");
            scanner.nextLine();
        }
    }

    private static void clearDatabase(Scanner scanner) {
        System.out.print("Xác nhận xóa toàn bộ records trong DB? (y/N): ");
        String confirm = scanner.nextLine().trim();
        if (!"y".equalsIgnoreCase(confirm)) {
            System.out.println("Đã hủy.");
            return;
        }
        try (DatabaseManager db = new DatabaseManager(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASS)) {
            db.truncateTable();
            System.out.println("Đã xóa toàn bộ dữ liệu trong bảng records.");
        } catch (Exception e) {
            System.err.println("Lỗi khi xóa dữ liệu: " + e.getMessage());
        }
    }

    private static int readInt(Scanner scanner, String label, int defaultValue) {
        System.out.printf("%s [mặc định %d]: ", label, defaultValue);
        String input = scanner.nextLine().trim();
        if (input.isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(input);
        } catch (NumberFormatException e) {
            System.out.printf("Giá trị không hợp lệ, dùng mặc định %d.%n", defaultValue);
            return defaultValue;
        }
    }

    private static void runSimulation(String host, int port, String serverName, boolean multithread,
                                      int numClients, int recordsPerClient) throws Exception {
        int totalRecords = numClients * recordsPerClient;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount   = new AtomicInteger(0);
        AtomicInteger sentCount    = new AtomicInteger(0);

        System.out.printf("%nĐang gửi %d records (%d clients × %d records/client) tới %s server...%n",
                totalRecords, numClients, recordsPerClient, serverName);

        ExecutorService pool  = Executors.newFixedThreadPool(numClients);
        CountDownLatch  latch = new CountDownLatch(numClients);

        Thread progressThread = startProgressPrinter(sentCount, totalRecords);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numClients; i++) {
            final String clientId = "client-" + i;
            pool.submit(() -> {
                try {
                    sendRecords(host, port, clientId, recordsPerClient, successCount, errorCount, sentCount);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long kafkaElapsed = System.currentTimeMillis() - startTime;

        progressThread.interrupt();
        progressThread.join(500);
        System.out.printf("\r[██████████████████████████████] 100%% (%d/%d)%n", totalRecords, totalRecords);
        pool.shutdown();

        int success = successCount.get();
        int error   = errorCount.get();

        System.out.println();
        System.out.println("========== KẾT QUẢ ==========");
        System.out.printf("Server       : %s (%s:%d)%n", serverName, host, port);
        System.out.printf("Total sent   : %d%n", totalRecords);
        System.out.printf("Success      : %d | Error: %d%n", success, error);

        if (multithread && success > 0) {
            // Multithread: "OK" = vào Kafka, chưa phải DB → poll DB đến khi đủ records
            System.out.printf("Kafka push   : %d ms (%.1f rec/s)%n",
                    kafkaElapsed, success * 1000.0 / kafkaElapsed);
            System.out.print("Chờ DB insert hoàn tất");
            long dbElapsed = waitForDbInsert(success, startTime);
            System.out.printf("%nDB insert    : %d ms (%.1f rec/s)%n",
                    dbElapsed, success * 1000.0 / dbElapsed);
        } else {
            // Single thread: "OK" = đã insert DB → elapsed là thời gian thật
            System.out.printf("DB insert    : %d ms (%.1f rec/s)%n",
                    kafkaElapsed, success * 1000.0 / Math.max(1, kafkaElapsed));
        }

        System.out.printf("Success rate : %.1f%%%n", success * 100.0 / Math.max(1, totalRecords));
        System.out.println("==============================");
    }

    private static long waitForDbInsert(int expectedCount, long startTime) {
        try (DatabaseManager db = new DatabaseManager(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASS)) {
            long deadline = System.currentTimeMillis() + 60_000; // timeout 60s
            while (System.currentTimeMillis() < deadline) {
                long current = db.countRecords();
                if (current >= expectedCount) break;
                System.out.print(".");
                System.out.flush();
                Thread.sleep(200);
            }
        } catch (Exception e) {
            System.err.printf("%nLỗi poll DB: %s%n", e.getMessage());
        }
        return System.currentTimeMillis() - startTime;
    }

    private static Thread startProgressPrinter(AtomicInteger sentCount, int totalRecords) {
        Thread t = new Thread(() -> {
            int barWidth = 30;
            while (!Thread.currentThread().isInterrupted()) {
                int sent = sentCount.get();
                int filled = totalRecords > 0 ? Math.min(sent * barWidth / totalRecords, barWidth) : 0;
                StringBuilder bar = new StringBuilder("[");
                for (int i = 0; i < barWidth; i++) bar.append(i < filled ? '█' : '░');
                bar.append("]");
                int pct = totalRecords > 0 ? Math.min(sent * 100 / totalRecords, 100) : 0;
                System.out.printf("\r%s %3d%% (%d/%d)  ", bar, pct, sent, totalRecords);
                System.out.flush();
                if (sent >= totalRecords) break;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void sendRecords(String host, int port, String clientId, int recordsPerClient,
                                    AtomicInteger successCount, AtomicInteger errorCount,
                                    AtomicInteger sentCount) {
        int localSent = 0;
        try (Socket socket = new Socket(host, port);
             PrintWriter    out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            for (int i = 0; i < recordsPerClient; i++) {
                String json = toJson(UUID.randomUUID().toString(), "payload-" + clientId + "-" + i,
                        System.currentTimeMillis(), clientId);
                out.println(json);

                String response = in.readLine();
                localSent++;
                sentCount.incrementAndGet();
                if ("OK".equals(response)) {
                    successCount.incrementAndGet();
                } else {
                    errorCount.incrementAndGet();
                }
            }
        } catch (Exception e) {
            int remaining = recordsPerClient - localSent;
            errorCount.addAndGet(remaining);
            sentCount.addAndGet(remaining);
            System.err.printf("%n[%s] Connection error after %d/%d: %s%n",
                    clientId, localSent, recordsPerClient, e.getMessage());
        }
    }

    private static String toJson(String id, String payload, long timestamp, String clientId) {
        try {
            return MAPPER.writeValueAsString(new RecordDto(id, payload, timestamp, clientId));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    record RecordDto(String id, String payload, long timestamp, String clientId) {}
}
