package com.rtf.redis.client.lb;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * redis主机更新
 * @Author : liupeng
 * @Date : 2020-02-10
 * @Modified By
 */
@Slf4j
public class AppRedisHostUpdater {

    private static ScheduledThreadPoolExecutor redisHostListRefreshExecutor = null ;

    static {
        ThreadFactory factory = (new ThreadFactoryBuilder())
                .setNameFormat("RedisHostListUpdater-%d")
                .setDaemon(true)
                .build();

        int coreSize = Math.max( 2 , Runtime.getRuntime().availableProcessors()/2 ) ;

        redisHostListRefreshExecutor = new ScheduledThreadPoolExecutor( coreSize , factory );
    }

    private static ScheduledFuture scheduledFuture = null ;

    private final AtomicBoolean isActive = new AtomicBoolean(false);

    private long initialDelay = 60 ;

    private long delay = 120 ;

    public synchronized void start(final AppRedisHostUpdateAction appRedisHostUpdateAction) {
        if ( isActive.compareAndSet(false, true) ) {
            final Runnable wrapperRunnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        appRedisHostUpdateAction.doUpdate() ;
                    } catch (Exception e) {
                        log.error("更新redis地址列表失败", e) ;
                    }
                }
            } ;
            scheduledFuture = redisHostListRefreshExecutor.scheduleWithFixedDelay( wrapperRunnable ,
                    initialDelay , delay , TimeUnit.SECONDS ) ;
        } else {
            log.debug("更新redis地址列表任务已经启动");
        }
    }

}
