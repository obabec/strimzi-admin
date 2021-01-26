package io.strimzi.admin.systemtest;

import io.vertx.core.Vertx;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DynamicWait {
    public static  <T> void waitFor(Callable<T> func, T expected, int timeInSec) throws Exception {
        int attempts = 0;
        AtomicBoolean ready = new AtomicBoolean(false);
        while (attempts++ < timeInSec && !ready.get()) {
            T b = func.call();
            if (b == expected) {
                ready.set(true);
            }
            Thread.sleep(1000);
        }
        if (!ready.get()) {
            throw new TimeoutException();
        }
    }
}
