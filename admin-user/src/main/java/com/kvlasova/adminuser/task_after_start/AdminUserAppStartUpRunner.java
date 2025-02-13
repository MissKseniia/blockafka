package com.kvlasova.adminuser.task_after_start;

import com.kvlasova.adminuser.service.AdminClientService;
import com.kvlasova.adminuser.service.AdminUserService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AdminUserAppStartUpRunner implements ApplicationRunner {

    private final AdminUserService adminUserService;
    private final AdminClientService adminClientService;

    @Override
    public void run(ApplicationArguments args) throws InterruptedException {
        if (adminClientService.createAllTopics()) {
            adminUserService.fillUserInfo();
            adminUserService.updateCenzWords();
            Thread.sleep(120000L);
            adminUserService.updateCenzWords();
            adminUserService.closeWordsProducer();
            adminUserService.closeUserInfoProducer();
        }

    }
}
