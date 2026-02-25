package com.cdc.destination.parquet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

public class ParquetFileManager {
    private static final Logger log = LoggerFactory.getLogger(ParquetFileManager.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final Path basePath;
    private final int fileSizeLimitMb;
    private final AtomicInteger fileSequence = new AtomicInteger(0);

    public ParquetFileManager(String basePath, int fileSizeLimitMb) {
        this.basePath = Path.of(basePath);
        this.fileSizeLimitMb = fileSizeLimitMb;
    }

    public Path getNextFilePath(String tableName) throws IOException {
        String date = LocalDate.now().format(DATE_FORMAT);
        Path dir = basePath.resolve(tableName).resolve(date);
        Files.createDirectories(dir);

        int seq = fileSequence.incrementAndGet();
        String fileName = String.format("part-%05d.parquet", seq);
        Path filePath = dir.resolve(fileName);

        log.debug("Next parquet file: {}", filePath);
        return filePath;
    }

    public boolean shouldRotate(Path currentFile) {
        if (currentFile == null || !Files.exists(currentFile)) return false;
        try {
            long sizeBytes = Files.size(currentFile);
            long sizeMb = sizeBytes / (1024 * 1024);
            return sizeMb >= fileSizeLimitMb;
        } catch (IOException e) {
            return false;
        }
    }

    public Path getBasePath() {
        return basePath;
    }
}
