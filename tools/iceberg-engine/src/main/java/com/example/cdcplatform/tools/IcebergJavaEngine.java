package com.example.cdcplatform.tools;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

public final class IcebergJavaEngine {
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile(
        "(?is)\\b(?:CREATE\\s+TABLE\\s+IF\\s+NOT\\s+EXISTS|INSERT\\s+INTO)\\s+([A-Za-z0-9_.]+)"
    );

    private IcebergJavaEngine() {
    }

    public static void main(String[] args) throws Exception {
        String sql = new String(System.in.readAllBytes(), StandardCharsets.UTF_8).trim();
        if (sql.isEmpty()) {
            throw new IllegalArgumentException("SQL stdin is required");
        }

        EngineContext context = EngineContext.from(sql);
        if (sql.toUpperCase(Locale.ROOT).startsWith("CREATE TABLE")) {
            bootstrap(context);
            return;
        }
        if (sql.toUpperCase(Locale.ROOT).startsWith("INSERT INTO")) {
            append(context, sql);
            return;
        }
        throw new IllegalArgumentException("Unsupported SQL for local Iceberg Java engine");
    }

    private static void bootstrap(EngineContext context) {
        HadoopCatalog catalog = context.catalog();
        ensureNamespace(catalog, context.namespace());
        Table table;
        if (catalog.tableExists(context.tableIdentifier())) {
            table = catalog.loadTable(context.tableIdentifier());
        } else {
            table = catalog.createTable(
                context.tableIdentifier(),
                icebergSchema(),
                icebergSpec(),
                tableProperties()
            );
        }

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("engine", "iceberg-java-api");
        summary.put("action", "bootstrap");
        summary.put("tableIdentifier", context.tableIdentifier().toString());
        summary.put("tableLocation", table.location());
        summary.put("metadataLocation", latestMetadataFile(table.location()));
        summary.put("formatVersion", 2);
        summary.put("snapshotCount", countSnapshots(table));
        System.out.println(toJson(summary));
    }

    private static void append(EngineContext context, String sql) throws IOException {
        HadoopCatalog catalog = context.catalog();
        Table table = catalog.loadTable(context.tableIdentifier());
        List<EventRecord> events = parseInsertRows(sql);
        if (events.isEmpty()) {
            throw new IllegalArgumentException("INSERT SQL did not contain any value rows");
        }
        LocalDate partitionDate = events.get(0).eventDate();
        for (EventRecord event : events) {
            if (!partitionDate.equals(event.eventDate())) {
                throw new IllegalArgumentException("This local proof writer expects one event_date partition per append");
            }
        }

        Path dataFilePath = dataFilePath(table.location(), partitionDate);
        java.nio.file.Files.createDirectories(dataFilePath.getParent());
        OutputFile outputFile = org.apache.iceberg.Files.localOutput(dataFilePath.toFile());
        PartitionData partition = new PartitionData(table.spec().partitionType());
        partition.set(0, DateTimeUtil.daysFromDate(partitionDate));
        DataWriter<Record> writer = Parquet.writeData(outputFile)
            .forTable(table)
            .createWriterFunc(GenericParquetWriter::create)
            .withSpec(table.spec())
            .withPartition(partition)
            .overwrite()
            .build();
        try (DataWriter<Record> closeableWriter = writer) {
            for (EventRecord event : events) {
                closeableWriter.write(event.toRecord(table.schema()));
            }
        }

        DataFile dataFile = writer.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("engine", "iceberg-java-api");
        summary.put("action", "append");
        summary.put("tableIdentifier", context.tableIdentifier().toString());
        summary.put("tableLocation", table.location());
        summary.put("metadataLocation", latestMetadataFile(table.location()));
        summary.put("formatVersion", 2);
        summary.put("snapshotId", snapshot == null ? null : snapshot.snapshotId());
        summary.put("snapshotCount", countSnapshots(table));
        summary.put("appendedRecordCount", events.size());
        summary.put("dataFile", dataFilePath.toString());
        System.out.println(toJson(summary));
    }

    private static Schema icebergSchema() {
        return new Schema(
            Types.NestedField.required(1, "event_id", Types.StringType.get()),
            Types.NestedField.required(2, "event_date", Types.DateType.get()),
            Types.NestedField.required(3, "source_table", Types.StringType.get()),
            Types.NestedField.required(4, "source_primary_key", Types.StringType.get()),
            Types.NestedField.required(5, "operation", Types.StringType.get()),
            Types.NestedField.optional(6, "stage", Types.StringType.get()),
            Types.NestedField.optional(7, "agent_task_status", Types.StringType.get())
        );
    }

    private static PartitionSpec icebergSpec() {
        return PartitionSpec.builderFor(icebergSchema())
            .identity("event_date")
            .build();
    }

    private static Map<String, String> tableProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "2");
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name().toLowerCase(Locale.ROOT));
        return properties;
    }

    private static void ensureNamespace(HadoopCatalog catalog, Namespace namespace) {
        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
        }
    }

    private static int countSnapshots(Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private static Path dataFilePath(String tableLocation, LocalDate partitionDate) {
        Path tablePath = tablePath(tableLocation);
        return tablePath
            .resolve("data")
            .resolve("event_date=" + partitionDate)
            .resolve("part-" + System.currentTimeMillis() + ".parquet");
    }

    private static Path tablePath(String tableLocation) {
        URI uri = URI.create(tableLocation.endsWith("/") ? tableLocation : tableLocation + "/");
        return "file".equals(uri.getScheme()) ? Path.of(uri) : Path.of(tableLocation);
    }

    private static String latestMetadataFile(String tableLocation) {
        Path metadataDir = tablePath(tableLocation).resolve("metadata");
        try (var paths = java.nio.file.Files.list(metadataDir)) {
            return paths
                .filter(path -> path.getFileName().toString().endsWith(".metadata.json"))
                .max((left, right) -> {
                    try {
                        return java.nio.file.Files.getLastModifiedTime(left)
                            .compareTo(java.nio.file.Files.getLastModifiedTime(right));
                    } catch (IOException exc) {
                        return left.toString().compareTo(right.toString());
                    }
                })
                .map(Path::toString)
                .orElse(null);
        } catch (IOException exc) {
            return null;
        }
    }

    private static List<EventRecord> parseInsertRows(String sql) {
        String values = sql.substring(sql.toUpperCase(Locale.ROOT).indexOf("VALUES") + "VALUES".length());
        Matcher matcher = Pattern.compile("\\(([^()]*)\\)", Pattern.DOTALL).matcher(values);
        List<EventRecord> events = new ArrayList<>();
        while (matcher.find()) {
            List<String> fields = splitRow(matcher.group(1));
            if (fields.size() != 7) {
                throw new IllegalArgumentException("Expected 7 INSERT values per row but got " + fields.size());
            }
            events.add(new EventRecord(
                unquote(fields.get(0)),
                LocalDate.parse(unquote(fields.get(1).replaceFirst("(?i)^DATE\\s+", ""))),
                unquote(fields.get(2)),
                unquote(fields.get(3)),
                unquote(fields.get(4)),
                unquote(fields.get(5)),
                unquote(fields.get(6))
            ));
        }
        return events;
    }

    private static List<String> splitRow(String row) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int index = 0; index < row.length(); index++) {
            char ch = row.charAt(index);
            if (ch == '\'') {
                current.append(ch);
                if (quoted && index + 1 < row.length() && row.charAt(index + 1) == '\'') {
                    current.append(row.charAt(index + 1));
                    index++;
                } else {
                    quoted = !quoted;
                }
                continue;
            }
            if (ch == ',' && !quoted) {
                values.add(current.toString().trim());
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        values.add(current.toString().trim());
        return values;
    }

    private static String unquote(String value) {
        String trimmed = value.trim();
        if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
            return trimmed.substring(1, trimmed.length() - 1).replace("''", "'");
        }
        return trimmed;
    }

    private static String toJson(Map<String, Object> values) {
        StringBuilder builder = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (!first) {
                builder.append(',');
            }
            first = false;
            builder.append('"').append(escapeJson(entry.getKey())).append('"').append(':');
            Object value = entry.getValue();
            if (value == null) {
                builder.append("null");
            } else if (value instanceof Number || value instanceof Boolean) {
                builder.append(value);
            } else {
                builder.append('"').append(escapeJson(String.valueOf(value))).append('"');
            }
        }
        builder.append('}');
        return builder.toString();
    }

    private static String escapeJson(String value) {
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    private record EventRecord(
        String eventId,
        LocalDate eventDate,
        String sourceTable,
        String sourcePrimaryKey,
        String operation,
        String stage,
        String agentTaskStatus
    ) {
        Record toRecord(Schema schema) {
            GenericRecord record = GenericRecord.create(schema);
            record.setField("event_id", eventId);
            record.setField("event_date", eventDate);
            record.setField("source_table", sourceTable);
            record.setField("source_primary_key", sourcePrimaryKey);
            record.setField("operation", operation);
            record.setField("stage", stage);
            record.setField("agent_task_status", agentTaskStatus);
            return record;
        }
    }

    private record EngineContext(Path warehouse, Namespace namespace, TableIdentifier tableIdentifier) {
        static EngineContext from(String sql) {
            String repoRoot = System.getProperty("user.dir");
            String warehouseValue = System.getenv().getOrDefault(
                "CDC_LAKEHOUSE_ICEBERG_WAREHOUSE",
                Path.of(repoRoot, "lakehouse", "out", "iceberg-java-engine", "warehouse").toString()
            );
            String identifierValue = tableIdentifierFrom(sql);
            String[] parts = identifierValue.split("\\.");
            if (parts.length < 2 || parts.length > 3) {
                throw new IllegalArgumentException("Expected table identifier as namespace.table or catalog.namespace.table");
            }
            String namespaceValue = parts.length == 2 ? parts[0] : parts[1];
            String tableValue = parts.length == 2 ? parts[1] : parts[2];
            Namespace namespace = Namespace.of(namespaceValue);
            return new EngineContext(
                Path.of(warehouseValue).toAbsolutePath(),
                namespace,
                TableIdentifier.of(namespace, tableValue)
            );
        }

        HadoopCatalog catalog() {
            return new HadoopCatalog(new Configuration(), warehouse.toString());
        }

        private static String tableIdentifierFrom(String sql) {
            Matcher matcher = IDENTIFIER_PATTERN.matcher(sql);
            if (!matcher.find()) {
                throw new IllegalArgumentException("Could not find table identifier in SQL");
            }
            return matcher.group(1);
        }
    }
}
