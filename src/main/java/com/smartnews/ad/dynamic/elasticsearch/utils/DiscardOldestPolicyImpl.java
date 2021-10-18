package com.smartnews.ad.dynamic.elasticsearch.utils;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class DiscardOldestPolicyImpl implements RejectedExecutionHandler {
    public DiscardOldestPolicyImpl() {
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (!executor.isShutdown()) {
            executor.getQueue().poll();
            System.out.println("Executor discard oldest task...");
            executor.execute(r);
        } else {
            System.out.println("Executor shutdown...");
        }
    }
}
