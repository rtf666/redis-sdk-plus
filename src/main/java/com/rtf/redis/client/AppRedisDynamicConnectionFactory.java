package com.rtf.redis.client;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.rtf.redis.client.lb.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConverters;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * redis连接代理
 * @Author : liupeng
 * @Date : 2020-02-10
 * @Modified By
 */
@Slf4j
public class AppRedisDynamicConnectionFactory
        implements InitializingBean, DisposableBean, RedisConnectionFactory, ReactiveRedisConnectionFactory {

    // 连接最大获取重试
    public static final int MAX_CONNECTION_RETRY_NUM  = 5 ;

    private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
            LettuceConverters.exceptionConverter());

    public static final String MASTER_SLAVE_SPLITTER  = ";" ;

    public static final String HOST_SPLITTER  = "," ;

    private RedisProperties properties ;

    private final List<AppRedisConnectionFactory> connectionFactories = Lists.newArrayList() ;

    private LettuceClientConfiguration clientConfig ;

    private AppRedisHostUpdater appRedisHostUpdater = new AppRedisHostUpdater() ;

    private AppRedisHostList appRedisHostList ;

    protected ReadWriteLock upServerLock = new ReentrantReadWriteLock() ;

    private AppRedisRoundRobinRule appRedisRoundRobinRule ;

    private String redisServerType = "redis" ;

    public AppRedisDynamicConnectionFactory(String type , RedisProperties properties ,
                                            LettuceClientConfiguration clientConfig ,
                                            AppRedisHostList initAppRedisHostList){

        this.redisServerType = type ;

        this.properties = properties ;

        this.clientConfig = clientConfig ;

        // 初始化redis服务拉取列表
        this.appRedisHostList = initAppRedisHostList ;
        if( this.appRedisHostList==null ){
            // 如果没有指定 appRedisHostList , 则使用默认的 appRedisHostList
            AppRedisHostListDefault appRedisHostListDefault = new AppRedisHostListDefault() ;
            appRedisHostListDefault.setProperties( properties ) ;
            this.appRedisHostList = appRedisHostListDefault ;
        }

        // 执行更新列表
        appRedisHostUpdater.start( ()->{
            String hosts = appRedisHostList.getUpdatedHostList() ;
            initConnectionFactories( parseMasterHosts( hosts ) , parseSlaveHosts( hosts ) ) ;
        } ) ;

        this.appRedisRoundRobinRule = new AppRedisRoundRobinRule( connectionFactories ) ;
    }

    /**
     * 获取配置信息
     * @param host
     * @return
     */
    private RedisStandaloneConfiguration getStandaloneConfig( String host ) {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();

        config.setHostName( host ) ;
        config.setPort(this.properties.getPort());
        config.setPassword(RedisPassword.of(this.properties.getPassword()));

        config.setDatabase(this.properties.getDatabase());
        return config;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initConnectionFactories( parseMasterHosts( properties.getHost() ) , parseSlaveHosts( properties.getHost() ) ) ;
    }

    /**
     * 解析slave节点
     * @param hosts
     * @return
     */
    private List<String> parseSlaveHosts(String hosts){
        String slaveHosts = null ;

        int msIndex = hosts.indexOf( MASTER_SLAVE_SPLITTER ) ;
        if( msIndex != -1 ){
            slaveHosts = hosts.substring( msIndex + 1 ) ;
        }

        return StringUtils.isBlank( slaveHosts ) ? null :
                Splitter.on( HOST_SPLITTER ).trimResults().omitEmptyStrings().splitToList( slaveHosts ) ;
    }

    /**
     * 解析master节点
     * @param hosts
     * @return
     */
    private List<String> parseMasterHosts(String hosts){
        String masterHosts = null ;

        int msIndex = hosts.indexOf( MASTER_SLAVE_SPLITTER ) ;
        if( msIndex != -1 ){
            masterHosts = hosts.substring( 0 ,msIndex ) ;
        }else{
            masterHosts = hosts ;
        }

        return Splitter.on( HOST_SPLITTER ).trimResults().omitEmptyStrings().splitToList( masterHosts ) ;
    }

    /**
     * 初始化redis连接工厂
     * @param masterHosts
     * @param slaveHosts
     */
    protected void initConnectionFactories( List<String> masterHosts , List<String> slaveHosts ){
        List<String> hosts = Lists.newArrayList() ;
        if( masterHosts!=null ){
            hosts.addAll( masterHosts ) ;
        }
        if( slaveHosts!=null ){
            hosts.addAll( slaveHosts ) ;
        }
        if( hosts==null || hosts.size()<1 ){
            log.debug("初始化redis连接为空");
            return;
        }

        Lock writeLock = null ;
        try{
            writeLock = upServerLock.writeLock() ;
            writeLock.lock();
            // 初始化redis连接
            for (String host : hosts) {
                if( hasConnectionFactory( host ) ){
                    continue;
                }
                log.debug("初始化redis连接: {}" , host);
                AppRedisConnectionFactory connectionFactory = buildConnectionFactory( host , clientConfig ,
                        masterHosts!=null && masterHosts.contains( host ) ) ;
                connectionFactories.add( connectionFactory ) ;
            }
            // 找出无用的redis连接
            List<AppRedisConnectionFactory> removeAppLettuceConnectionFactory = Lists.newArrayList() ;
            for (AppRedisConnectionFactory connectionFactory : connectionFactories) {
                if( hosts.contains( connectionFactory.getHostName() ) ){
                    continue;
                }
                removeAppLettuceConnectionFactory.add( connectionFactory ) ;
            }
            // 移除无用的redis连接
            connectionFactories.removeAll( removeAppLettuceConnectionFactory ) ;
            // 销毁无用的redis连接
            destroy( removeAppLettuceConnectionFactory ) ;
        }catch( Exception e ){
            log.error( "初始化redis连接工厂异常: {}" , e ) ;
        }finally {
            if(writeLock!=null){
                writeLock.unlock();
            }
        }
    }

    /**
     * 判断指定的host是否已经存在初始化的连接工厂
     * @param host
     * @return
     */
    protected boolean hasConnectionFactory( String host ){
        for (AppRedisConnectionFactory factory : connectionFactories) {
            if(StringUtils.equalsIgnoreCase( factory.getHostName(), host )){
                return true ;
            }
        }
        return false ;
    }

    /**
     * 构建redis连接工厂
     * @param host
     * @param clientConfig
     * @return
     */
    protected AppRedisConnectionFactory buildConnectionFactory(String host,
                                                               LettuceClientConfiguration clientConfig ,
                                                               boolean isMaster){
        AppRedisConnectionFactory connectionFactory = new AppRedisConnectionFactory( redisServerType ,
                getStandaloneConfig(host) , clientConfig ) ;

        // 开启动态节点的健康检查
        connectionFactory.setEnableHealthCheck( true ) ;
        // 是否master节点
        connectionFactory.setMaster( isMaster ) ;

        connectionFactory.afterPropertiesSet() ;

        return connectionFactory ;
    }

    /**
     * 销毁redis连接工厂
     * @param destroyConnectionFactories
     */
    protected void destroy(List<AppRedisConnectionFactory> destroyConnectionFactories){
        if( destroyConnectionFactories==null || destroyConnectionFactories.size()<1 ){
            return;
        }
        for (AppRedisConnectionFactory destroyConnectionFactory : destroyConnectionFactories) {
            String hostName = destroyConnectionFactory.getHostName() ;
            try{
                log.debug("销毁redis连接池: {}" , hostName);
                destroyConnectionFactory.destroy();
            }catch( Exception e ){
                log.error( "销毁redis连接失败 {} : {}" ,  hostName , e ) ;
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        destroy( connectionFactories ) ;
    }

    @Override
    public RedisConnection getConnection() {
        int count = 0 ;

        RedisConnection redisConnection = null ;

        while (redisConnection==null && count++ < MAX_CONNECTION_RETRY_NUM){
            redisConnection = getUpConnection() ;
        }

        if( redisConnection==null ){
            throw new RuntimeException("无可用的redis连接") ;
        }

        return redisConnection ;
    }

    /**
     * 获取启用的连接
     * @return
     */
    public RedisConnection getUpConnection(){
        AppRedisConnectionFactory appCodisConnectionFactory = appRedisRoundRobinRule.choose(  null ) ;
        if( appCodisConnectionFactory==null ){
            return null ;
        }
        // 1. 获取熔断对象实例
        log.debug("选择redis主机: {}连接池" , appCodisConnectionFactory.getHostName() );
        AppRedisCircuitBreaker appRedisCircuitBreaker = AppRedisCircuitBreaker.getInstance( appCodisConnectionFactory.getHostName() ) ;

        RedisConnection redisConnection = null ;
        try{
            redisConnection = appCodisConnectionFactory.getConnection() ;
        }catch( Exception e ){
            log.error( "获取redis连接异常 {} : {}" , appCodisConnectionFactory.getHostName() , e ) ;
            appRedisCircuitBreaker.incrementSuccessiveFailureCount();
        }

        return redisConnection ;
    }

    @Override
    public ReactiveRedisConnection getReactiveConnection() {
//        AppCodisConnectionFactory appCodisConnectionFactory = appRedisRoundRobinRule.choose(  null ) ;
//        if( appCodisConnectionFactory==null ){
//            throw new AppException("无可用的redis连接池") ;
//        }
//        log.info("ReactiveRedisConnection 获取redis连接: {}" , appCodisConnectionFactory.getHostName());
//        return appCodisConnectionFactory.getReactiveConnection() ;
        throw new RuntimeException("不支持ReactiveRedisConnection") ;
    }

    @Override
    public RedisClusterConnection getClusterConnection() {
        throw new RuntimeException("not support RedisClusterConnection") ;
    }

    @Override
    public ReactiveRedisClusterConnection getReactiveClusterConnection() {
        throw new RuntimeException("not support ReactiveRedisClusterConnection") ;
    }

    @Override
    public boolean getConvertPipelineAndTxResults() {
        return true;
    }

    @Override
    public RedisSentinelConnection getSentinelConnection() {
        throw new RuntimeException("not support RedisSentinelConnection") ;
    }

    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
        return EXCEPTION_TRANSLATION.translate(ex);
    }
}
