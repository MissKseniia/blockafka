package com.kvlasova.utils;

import com.kvlasova.enums.Users;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@UtilityClass
public class UsersUtils {
    private static final Random random = new Random();

    public static List<String> getRandomBlockedUsers(String user) {
        var list =  Arrays.stream(Users.values())
                        .map(Users::getName)
                        .filter(u -> !user.equalsIgnoreCase(u))
                        .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(list, random);
        return list.subList(0, random.nextInt(list.size()-1));
    }

    public static int getRandomLimit() {
        return random.nextInt(0,11);
    }

    public static String getRandomUserToSend(String userSender) {
        return Users.getUsersNames().stream()
                .filter(u -> !userSender.equalsIgnoreCase(u))
                .skip(random.nextLong(0, userSender.length()-1))
                .findFirst().get();
    }

}
