package dev.nullzwo;

import java.time.Duration;

public record Throughput(int messageCount, Duration duration) {
}
