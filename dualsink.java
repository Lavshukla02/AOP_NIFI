package com.waisldigital;

import com.typesafe.config.Config;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class DualSinkProcessor extends ProcessFunction<Map<String, Object>, Map<String, Object>> {

    private static final Logger log = LoggerFactory.getLogger(DualSinkProcessor.class);

    private final Config config;
    private transient Connection connection;

    public DualSinkProcessor(Config config) {
        this.config = config;
    }

    // ================= INIT =================
    private void init() throws Exception {
        if (connection == null || connection.isClosed()) {

            log.info("Connecting to Postgres...");

            connection = DriverManager.getConnection(
                    config.getString("postgres.url"),
                    config.getString("postgres.user"),
                    config.getString("postgres.password")
            );

            connection.setAutoCommit(true);
        }
    }

    // ================= PROCESS =================
    @Override
    public void processElement(Map<String, Object> value, Context ctx, Collector<Map<String, Object>> out) {

        // forward to Kafka always
        out.collect(value);

        try {
            init();

            if (value == null || value.isEmpty()) {
                log.warn("Empty record");
                return;
            }

            writeDynamic(value);

        } catch (Exception e) {
            log.error("DB ERROR: {}", value, e);
        }
    }

    // ================= MAIN LOGIC =================
    private void writeDynamic(Map<String, Object> data) throws Exception {

        String schema = config.getString("postgres.schema");

        String table = (String) data.get("target_table");

        if (table == null || table.isEmpty()) {
            log.error("target_table missing: {}", data);
            return;
        }

        table = cleanColumn(table); // ✅ sanitize table name

        Map<String, Object> cleaned = new LinkedHashMap<>();

        for (Map.Entry<String, Object> e : data.entrySet()) {

            if (!e.getKey().equals("target_table")) {

                String col = cleanColumn(e.getKey());
                Object val = cleanValue(e.getValue());

                cleaned.put(col, val);
            }
        }

        if (cleaned.isEmpty()) {
            log.warn("No valid columns");
            return;
        }

        // 🔥 IMPORTANT: order matters
        createTable(schema, table, cleaned);
        addColumns(schema, table, cleaned);
        insert(schema, table, cleaned);
    }

    // ================= CREATE TABLE =================
    private void createTable(String schema, String table, Map<String, Object> data) throws Exception {

        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(schema).append(".").append(table).append(" (");

        for (String col : data.keySet()) {
            sql.append(col).append(" TEXT,");
        }

        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    // ================= ADD COLUMNS =================
    private void addColumns(String schema, String table, Map<String, Object> data) throws Exception {

        Set<String> existing = new HashSet<>();

        String query = "SELECT column_name FROM information_schema.columns WHERE table_schema=? AND table_name=?";

        try (PreparedStatement ps = connection.prepareStatement(query)) {

            ps.setString(1, schema);
            ps.setString(2, table);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                existing.add(rs.getString(1));
            }
        }

        for (String col : data.keySet()) {

            if (!existing.contains(col)) {

                String alter = "ALTER TABLE " + schema + "." + table +
                        " ADD COLUMN " + col + " TEXT";

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(alter);
                    log.info("Added column: {}", col);
                }
            }
        }
    }

    // ================= INSERT =================
    private void insert(String schema, String table, Map<String, Object> data) throws Exception {

        String cols = String.join(",", data.keySet());
        String vals = String.join(",", data.keySet().stream().map(k -> "?").toList());

        String sql = "INSERT INTO " + schema + "." + table +
                " (" + cols + ") VALUES (" + vals + ")";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {

            int i = 1;
            for (Object v : data.values()) {
                ps.setObject(i++, v);
            }

            ps.executeUpdate();
        }
    }

    // ================= CLEAN COLUMN =================
    private String cleanColumn(String col) {
        return col.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    // ================= CLEAN VALUE =================
    private Object cleanValue(Object val) {

        if (val == null) return null;

        String str = val.toString();

        // remove garbage delimiter
        if (str.contains("%$#^&@")) {
            str = str.split("%\\$#\\^&@")[0];
        }

        return str.replaceAll("[^a-zA-Z0-9_:\\-\\.\\s]", "");
    }
}
