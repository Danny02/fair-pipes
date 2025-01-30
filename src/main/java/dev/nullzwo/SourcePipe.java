package dev.nullzwo;

import java.util.Optional;

public interface SourcePipe {
    PipeId id();

    Message nextMessage();

    Optional<Message> tryNextMessage();
}
