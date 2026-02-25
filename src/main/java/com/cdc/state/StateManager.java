package com.cdc.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class StateManager {
    private static final Logger log = LoggerFactory.getLogger(StateManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path statePath;
    private SyncState state;

    public StateManager(Path statePath) {
        this.statePath = statePath;
    }

    public SyncState load() throws IOException {
        if (Files.exists(statePath)) {
            log.info("Loading state from {}", statePath);
            state = MAPPER.readValue(statePath.toFile(), SyncState.class);
        } else {
            log.info("No existing state file found, starting fresh");
            state = new SyncState();
        }
        return state;
    }

    public void save() throws IOException {
        // Atomic write: write to temp file then rename
        Path tempPath = statePath.resolveSibling(statePath.getFileName() + ".tmp");
        Files.createDirectories(statePath.getParent());
        MAPPER.writeValue(tempPath.toFile(), state);
        Files.move(tempPath, statePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        log.debug("State saved to {}", statePath);
    }

    public SyncState getState() {
        return state;
    }
}
