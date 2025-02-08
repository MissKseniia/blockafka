package com.kvlasova.processmessagecenter.task_after_start;

import com.kvlasova.processmessagecenter.service.BlockMessageService;
import com.kvlasova.processmessagecenter.service.FilterForbiddenWordsService;
import com.kvlasova.processmessagecenter.service.ProcessMessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ProcessMessageAppStartUpRunner implements ApplicationRunner {

    private final BlockMessageService blockMessageService;
    private final FilterForbiddenWordsService filterForbiddenWordsService;
    private final ProcessMessageService processMessageService;

    @Override
    public void run(ApplicationArguments args) {
        blockMessageService.blockUsers();
        filterForbiddenWordsService.cenzMessageContent();

        while(!processMessageService.areAllMessagesProcessed()) {
            continue;
        }
        blockMessageService.closeStreams();
        filterForbiddenWordsService.closeStreamsForCenz();

    }
}
