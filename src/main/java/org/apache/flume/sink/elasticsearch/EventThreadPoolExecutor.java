package org.apache.flume.sink.elasticsearch;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liz on 15/11/10.
 */
public class EventThreadPoolExecutor extends ThreadPoolExecutor {

    public boolean hasFinish = false;

    public AtomicInteger finishNum = new AtomicInteger(0);

    public EventThreadPoolExecutor(int i, int i1, long l, TimeUnit timeUnit, BlockingQueue<Runnable> blockingQueue) {
        super(i, i1, l, timeUnit, blockingQueue);
    }

    public EventThreadPoolExecutor(int i, int i1, long l, TimeUnit timeUnit, BlockingQueue<Runnable> blockingQueue, ThreadFactory threadFactory) {
        super(i, i1, l, timeUnit, blockingQueue, threadFactory);
    }

    public EventThreadPoolExecutor(int i, int i1, long l, TimeUnit timeUnit, BlockingQueue<Runnable> blockingQueue, RejectedExecutionHandler rejectedExecutionHandler) {
        super(i, i1, l, timeUnit, blockingQueue, rejectedExecutionHandler);
    }

    public EventThreadPoolExecutor(int i, int i1, long l, TimeUnit timeUnit, BlockingQueue<Runnable> blockingQueue, ThreadFactory threadFactory, RejectedExecutionHandler rejectedExecutionHandler) {
        super(i, i1, l, timeUnit, blockingQueue, threadFactory, rejectedExecutionHandler);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t){
        super.afterExecute(r, t);
        synchronized(this){
            if(this.getActiveCount() == 1 )
            {
                this.hasFinish=true;
                this.notify();
            }
        }
    }

    public void isEndTask() throws InterruptedException {
        synchronized(this){
            while (this.hasFinish == false) {
                    this.wait();
            }
        }
    }

    public void runOver(){
        finishNum.getAndIncrement();
    }

    public void reset(){
        finishNum.set(0);
        hasFinish = false;
    }

    public int getFinishNum(){
        return finishNum.get();
    }
}
