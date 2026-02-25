package com.cdc.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Path;

public class ConfigLoader {
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    public static SourceConfig loadSource(Path path) throws IOException {
        return YAML_MAPPER.readValue(path.toFile(), SourceConfig.class);
    }

    public static DestinationConfig loadDestination(Path path) throws IOException {
        return YAML_MAPPER.readValue(path.toFile(), DestinationConfig.class);
    }

    public static CatalogConfig loadCatalog(Path path) throws IOException {
        return YAML_MAPPER.readValue(path.toFile(), CatalogConfig.class);
    }
}
