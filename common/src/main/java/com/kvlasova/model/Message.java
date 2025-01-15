package com.kvlasova.model;


public record Message(
        String content,
        String receiver,
        String sender,
        boolean isBlocked
) {
}
