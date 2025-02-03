package dev.nullzwo.pipe;

import dev.nullzwo.Message;
import io.vavr.control.Option;

public interface SourcePipe {
    PipeId id();

    default Message nextMessage() {
        Option<Message> msg;
        while((msg = tryNextMessage()).isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return msg.get();
    }

    Option<Message> tryNextMessage();
}
