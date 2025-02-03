package dev.nullzwo.pipe;

import dev.nullzwo.Message;

public interface SinkPipe {
    PipeId id();

    void consume(Message message);
}
