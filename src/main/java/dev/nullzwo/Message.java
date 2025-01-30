package dev.nullzwo;

import java.time.Instant;

public interface Message {
    PipeId origin();
    Instant timestamp();
}
