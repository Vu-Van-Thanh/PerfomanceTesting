package com.testvt.db;

import com.testvt.model.Record;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class DatabaseManager implements AutoCloseable {

    private final HikariDataSource dataSource;

    public DatabaseManager(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30_000);
        config.setIdleTimeout(600_000);
        config.setMaxLifetime(1_800_000);
        config.setPoolName("BatchInsertPool");
        this.dataSource = new HikariDataSource(config);
    }

    public void createTableIfNotExists() throws SQLException {
        String ddl = """
                IF NOT EXISTS (
                    SELECT * FROM sysobjects WHERE name='records' AND xtype='U'
                )
                CREATE TABLE records (
                    id          VARCHAR(36)  NOT NULL PRIMARY KEY,
                    client_id   VARCHAR(50)  NOT NULL,
                    payload     VARCHAR(500) NOT NULL,
                    ts          BIGINT       NOT NULL,
                    inserted_at DATETIME2    DEFAULT GETDATE()
                )
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(ddl)) {
            ps.execute();
        }
    }

    /**
     * Batch insert: one INSERT with N rows — single round-trip to DB.
     */
    public int insertBatch(List<Record> batch) throws SQLException {
        if (batch.isEmpty()) return 0;

        StringBuilder sql = new StringBuilder(
                "INSERT INTO records (id, client_id, payload, ts, thread_id) VALUES ");
        for (int i = 0; i < batch.size(); i++) {
            sql.append("(?, ?, ?, ?, ?)");
            if (i < batch.size() - 1) sql.append(", ");
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {

            int idx = 1;
            for (Record r : batch) {
                ps.setString(idx++, r.getId());
                ps.setString(idx++, r.getClientId());
                ps.setString(idx++, r.getPayload());
                ps.setLong(idx++, r.getTimestamp());
                ps.setLong(idx++, r.getThreadId());
            }
            return ps.executeUpdate();
        }
    }

    public long countRecords() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM records")) {
            var rs = ps.executeQuery();
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    public void truncateTable() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("TRUNCATE TABLE records")) {
            ps.execute();
        }
    }

    /**
     * Single insert — used for comparison benchmark.
     */
    public int insertSingle(Record r) throws SQLException {
        String sql = "INSERT INTO records (id, client_id, payload, ts, thread_id) VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, r.getId());
            ps.setString(2, r.getClientId());
            ps.setString(3, r.getPayload());
            ps.setLong(4, r.getTimestamp());
            ps.setLong(5, r.getThreadId());
            return ps.executeUpdate();
        }
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
}
