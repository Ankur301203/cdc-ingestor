package com.cdc.cli;

import com.cdc.config.ConfigLoader;
import com.cdc.config.SourceConfig;
import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.Column;
import com.cdc.source.postgres.PostgresConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.*;

@Command(name = "discover", description = "Discover available tables and schemas")
public class DiscoverCommand implements Runnable {

    @Option(names = "--source", required = true, description = "Path to source config YAML")
    private Path sourcePath;

    @Override
    public void run() {
        try {
            SourceConfig config = ConfigLoader.loadSource(sourcePath);
            PostgresConnector connector = new PostgresConnector();
            List<CdcSchema> schemas = connector.discover(config);

            // Output as JSON
            ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            List<Map<String, Object>> output = new ArrayList<>();

            for (CdcSchema schema : schemas) {
                Map<String, Object> tableInfo = new LinkedHashMap<>();
                tableInfo.put("table", schema.fullTableName());

                List<Map<String, Object>> columns = new ArrayList<>();
                for (Column col : schema.columns()) {
                    Map<String, Object> colInfo = new LinkedHashMap<>();
                    colInfo.put("name", col.name());
                    colInfo.put("type", col.type().name());
                    colInfo.put("nullable", col.nullable());
                    colInfo.put("primary_key", col.primaryKey());
                    columns.add(colInfo);
                }
                tableInfo.put("columns", columns);
                output.add(tableInfo);
            }

            System.out.println(mapper.writeValueAsString(output));
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }
}
