package com.testvt.model;

public class Record {
    private final String id;
    private final String payload;
    private final long timestamp;
    private final String clientId;
    private final long threadId;

    public Record(String id, String payload, long timestamp, String clientId, long threadId) {
        this.id = id;
        this.payload = payload;
        this.timestamp = timestamp;
        this.clientId = clientId;
        this.threadId = threadId;
    }

    public String getId()       { return id; }
    public String getPayload()  { return payload; }
    public long getTimestamp()  { return timestamp; }
    public String getClientId() { return clientId; }
    public long getThreadId()   { return threadId; }

    @Override
    public String toString() {
        return "Record{id='" + id + "', clientId='" + clientId + "', threadId=" + threadId + ", ts=" + timestamp + "}";
    }
}
