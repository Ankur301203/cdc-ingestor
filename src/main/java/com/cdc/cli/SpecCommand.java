package com.cdc.cli;

import com.cdc.protocol.Connector;
import com.cdc.source.SourceConnectorFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "spec", description = "Show configuration specification")
public class SpecCommand implements Runnable {

    @Option(names = "--type", required = true, description = "Config type: source or destination")
    private String type;

    @Option(names = "--source-type", description = "When --type=source: source type (postgres or kafka)", defaultValue = "postgres")
    private String sourceType;

    @Override
    public void run() {
        switch (type.toLowerCase()) {
            case "source" -> {
                Connector connector = SourceConnectorFactory.forType(sourceType);
                System.out.println(connector.spec());
            }
            case "destination" -> {
                System.out.println("""
                    {
                      "type": "object",
                      "required": ["type"],
                      "properties": {
                        "type": { "type": "string", "enum": ["parquet", "iceberg"] },
                        "base_path": { "type": "string", "description": "Output directory for parquet files" },
                        "file_size_mb": { "type": "integer", "default": 256 },
                        "compression": { "type": "string", "default": "SNAPPY" },
                        "warehouse": { "type": "string", "description": "Iceberg warehouse path" },
                        "catalog_type": { "type": "string", "default": "hadoop" },
                        "catalog_properties": { "type": "object" },
                        "table_namespace": { "type": "string", "default": "cdc_db" }
                      }
                    }
                    """);
            }
            default -> System.err.println("Unknown type: " + type + ". Use 'source' or 'destination'.");
        }
    }
}
