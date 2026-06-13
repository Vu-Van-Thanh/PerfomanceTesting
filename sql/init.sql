-- Run this once on your SQL Server to set up the database

CREATE DATABASE TestVT;
GO

USE TestVT;
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='records' AND xtype='U')
CREATE TABLE records (
    id          VARCHAR(36)  NOT NULL PRIMARY KEY,
    client_id   VARCHAR(50)  NOT NULL,
    payload     VARCHAR(500) NOT NULL,
    ts          BIGINT       NOT NULL,
    thread_id   BIGINT       NOT NULL DEFAULT 0,
    inserted_at DATETIME2    DEFAULT GETDATE()
);
GO

-- Index for query performance
CREATE INDEX idx_records_client ON records(client_id);
CREATE INDEX idx_records_ts     ON records(ts);
GO
