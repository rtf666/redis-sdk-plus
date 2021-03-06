package com.rtf.redis.client.lb;

import com.rtf.redis.client.AppRedisConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@Slf4j
public class AppRedisHealthStats {
    // redis的健康检查key
    private static final String REDIS_CHECK_KEY  = "redis:check" ;

    private static RedisSerializer<String> stringSerializer = new StringRedisSerializer();

    private static ScheduledThreadPoolExecutor HEALTH_STATS_EXECUTOR = null ;

//    static {
//        ThreadFactory factory = (new ThreadFactoryBuilder())
//                .setNameFormat("RedisHealthCheck-%d")
//                .setDaemon(true)
//                .build();
//
//        int coreSize = Math.max( 20 , Runtime.getRuntime().availableProcessors() ) ;
//
//        HEALTH_STATS_EXECUTOR = new ScheduledThreadPoolExecutor( coreSize , factory );
//    }

    private AppRedisConnectionFactory connectionFactory ;

//    private InetUtils.HostInfo hostInfo ;

    private long initialDelay = 10 ;

    private long delay = 2 ;

    private ScheduledFuture scheduledFuture = null ;

    public AppRedisHealthStats( AppRedisConnectionFactory connectionFactory ){
        this.connectionFactory = connectionFactory ;
//        // 获取
//        hostInfo = UtilIp.getHostInfo() ;
//        log.info("本机网络信息，ip: {} " , hostInfo.getIpAddress()) ;
    }

    public void start(){
//        final String hostName = connectionFactory.getHostName() ;
//
//        final Runnable wrapperRunnable = ()->{
//            try {
//                log.info("{} redis健康检查开始:{}" , HEALTH_STATS_EXECUTOR.getActiveCount() , hostName) ;
//                // 1. 获取熔断对象实例
//                AppRedisCircuitBreaker appRedisCircuitBreaker = AppRedisCircuitBreaker.getInstance( hostName ) ;
//                // 2. 检查是否处于熔断状态，如果处于熔断状态则不再检查；否则开启检查
//                boolean circuitBreakerTripped = appRedisCircuitBreaker.isCircuitBreakerTripped() ;
//                if( circuitBreakerTripped ){
//                    log.info("{} redis执行熔断" , hostName);
//                    return;
//                }
//
//                boolean checkResult = check() ;
//                // 如果检查失败，则增加连续失败的次数；否则清空连续失败次数
//                if( checkResult ){
//                    appRedisCircuitBreaker.clearSuccessiveFailureCount() ;
//                }else{
//                    appRedisCircuitBreaker.incrementSuccessiveFailureCount() ;
//                }
//
//            } catch (Exception e) {
//                log.error("redis监控检查失败", e) ;
//            }
//        } ;
//
//        scheduledFuture = HEALTH_STATS_EXECUTOR.scheduleWithFixedDelay( wrapperRunnable ,
//                initialDelay , delay , TimeUnit.SECONDS ) ;
    }
    /**
     * 销毁检查任务
     */
    public void destory(){
        if( scheduledFuture==null ){
            return;
        }
        try{
            // 取消检查任务
            scheduledFuture.cancel(true) ;
        }catch( Exception e ){
            log.error( "redis连接工厂销毁异常 {} : {}" , connectionFactory.getHostName() , e.getMessage() ) ;
        }
    }
//
//    /**
//     * 检查连接工厂是否正常
//     * @return
//     */
//    public boolean check(){
//        try{
//            log.info("redis开始检查{}" , connectionFactory.getHostName());
//            // 当前时间索引
//            String currentTime = new DateTime().toString("yyyy-MM-dd HH:mm:ss") ;
//            // redis的key
//            String redisKey = REDIS_CHECK_KEY+":"+hostInfo.getIpAddress()+":"+connectionFactory.getHostName() ;
//            byte[] serializeKey = stringSerializer.serialize( redisKey ) ;
//            // redis的值
//            byte[] redisValue = stringSerializer.serialize( currentTime ) ;
//            // 有效时间，单位秒
//            long expireTargetTime = 30 ;
//            // 验证set 命令是否成功
//            boolean setSuccess = connectionFactory.getConnection().set( serializeKey , redisValue , Expiration.seconds( expireTargetTime ) ,
//                    RedisStringCommands.SetOption.UPSERT ) ;
//            // 验证get命令是否成功
//            redisValue = connectionFactory.getConnection().get( serializeKey ) ;
//            String redisValueStr = stringSerializer.deserialize( redisValue ) ;
//            boolean getSuccess = StringUtils.isNotBlank( redisValueStr ) ;
//
//            return setSuccess && getSuccess ;
//        }catch( Exception e ){
//            log.error( "redis健康检查出现异常  {} : {}" , connectionFactory.getHostName() , e.getMessage() ) ;
//        }
//        return false ;
//    }


}
