package dev.nullzwo;

import dev.nullzwo.pipe.PipeId;

import java.time.Instant;

public record Message(PipeId origin, Instant timestamp) {
}
