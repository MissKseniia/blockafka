package com.kvlasova.user.task_after_start;

import com.kvlasova.user.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
//@RequiredArgsConstructor
public class UserAppStartUpRunner implements ApplicationRunner {

    private final UserService userService;

    public UserAppStartUpRunner(@Autowired UserService userService) {
        this.userService = userService;
    }

    @Override
    public void run(ApplicationArguments args) {
        userService.exchangeMessages();
    }
}
