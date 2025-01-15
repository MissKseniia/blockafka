package com.kvlasova.model;

import java.util.List;

public record User(
        String userName,
        List<String> blockedUsers
) {
}
