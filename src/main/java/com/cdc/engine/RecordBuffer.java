package com.cdc.engine;

import com.cdc.protocol.Record;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RecordBuffer {
    private static final Record POISON_PILL = new Record("__STOP__", null, null, 0);
    private final ArrayBlockingQueue<Record> queue;

    public RecordBuffer(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    public void put(Record record) throws InterruptedException {
        queue.put(record);
    }

    public Record poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public void signalDone() throws InterruptedException {
        queue.put(POISON_PILL);
    }

    public boolean isDone(Record record) {
        return record == POISON_PILL;
    }

    public int size() {
        return queue.size();
    }
}
