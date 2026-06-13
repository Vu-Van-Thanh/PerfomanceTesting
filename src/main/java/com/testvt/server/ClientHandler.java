package com.testvt.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.testvt.kafka.RecordProducer;
import com.testvt.model.Record;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler implements Runnable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Socket         socket;
    private final RecordProducer producer;

    public ClientHandler(Socket socket, RecordProducer producer) {
        this.socket   = socket;
        this.producer = producer;
    }

    @Override
    public void run() {
        try (BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter    out = new PrintWriter(socket.getOutputStream(), true)) {

            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                try {
                    Record record = parse(line);
                    boolean ok = producer.send(record);
                    out.println(ok ? "OK" : "ERROR:SEND_FAILED");
                } catch (Exception e) {
                    out.println("ERROR:INVALID_JSON");
                }
            }
        } catch (Exception ignored) {
        } finally {
            try { socket.close(); } catch (Exception ignored) {}
        }
    }

    private Record parse(String json) throws Exception {
        JsonNode node = MAPPER.readTree(json);
        return new Record(
                node.get("id").asText(),
                node.get("payload").asText(),
                node.get("timestamp").asLong(),
                node.get("clientId").asText(),
                Thread.currentThread().getId()  // server worker thread ID
        );
    }
}
