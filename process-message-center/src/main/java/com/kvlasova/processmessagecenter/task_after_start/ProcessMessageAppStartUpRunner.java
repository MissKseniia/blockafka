package com.kvlasova.processmessagecenter.task_after_start;

import com.kvlasova.processmessagecenter.service.BlockMessageService;
import com.kvlasova.processmessagecenter.service.FilterForbiddenWordsService;
import com.kvlasova.processmessagecenter.service.ProcessMessageService;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ProcessMessageAppStartUpRunner implements ApplicationRunner {

    private final BlockMessageService blockMessageService;
    private final FilterForbiddenWordsService filterForbiddenWordsService;
    private final ProcessMessageService processMessageService;

    public ProcessMessageAppStartUpRunner(BlockMessageService blockMessageService, FilterForbiddenWordsService filterForbiddenWordsService, ProcessMessageService processMessageService) {
        this.blockMessageService = blockMessageService;
        this.filterForbiddenWordsService = filterForbiddenWordsService;
        this.processMessageService = processMessageService;
    }

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
