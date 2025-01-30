package dev.nullzwo;

public interface SinkPipe {
    PipeId id();

    void consume(Message message);

    int enqueued();
}
