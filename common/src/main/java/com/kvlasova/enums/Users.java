package com.kvlasova.enums;

import com.kvlasova.utils.UsersUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
public enum Users {
    USER1("user1", UsersUtils.getRandomLimit()),
    USER2("user2", UsersUtils.getRandomLimit()),
    USER3("user3", UsersUtils.getRandomLimit()),
    USER4("user4", UsersUtils.getRandomLimit()),
    USER5("user5", UsersUtils.getRandomLimit());

    private final String name;
    private final int limit;

    public static List<String> getUsersNames() {
        return Arrays.stream(Users.values()).map(Users::getName).collect(Collectors.toList());
    }

    public static int getUsersLimit() {
        return Arrays.stream(Users.values()).mapToInt(Users::getLimit).sum();
    }

}
