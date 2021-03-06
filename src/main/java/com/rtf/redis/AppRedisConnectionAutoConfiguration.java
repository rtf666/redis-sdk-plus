package com.rtf.redis;

import com.rtf.redis.client.AppCodisConnectionFactory;
import com.rtf.redis.client.AppRedisConnectionConfigurationAdaptor;
import com.rtf.redis.client.AppRedisDynamicConnectionFactory;
import com.rtf.redis.client.lb.AppRedisHostList;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Delay;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * redis自动化配置
 * @Author : liupeng
 * @Date : 2019-03-27
 * @Modified By
 */
@Slf4j
@Configuration
public class AppRedisConnectionAutoConfiguration extends AppRedisConnectionConfigurationAdaptor {

	/**
	 * 默认的超时连接时间
	 */
	private static final Integer DEFAULT_CONNECTION_TIMEOUT = 5 ;

	private final RedisProperties properties;

	private final List<LettuceClientConfigurationBuilderCustomizer> builderCustomizers;

	private AppRedisHostList appRedisHostList ;

	public AppRedisConnectionAutoConfiguration(RedisProperties properties,
											   ObjectProvider<AppRedisHostList> appRedisHostListObjectProvider,
											   ObjectProvider<RedisSentinelConfiguration> sentinelConfigurationProvider,
											   ObjectProvider<RedisClusterConfiguration> clusterConfigurationProvider,
											   ObjectProvider<List<LettuceClientConfigurationBuilderCustomizer>> builderCustomizers) {
		super(properties, sentinelConfigurationProvider, clusterConfigurationProvider);
		this.properties = properties;
		this.builderCustomizers = builderCustomizers
				.getIfAvailable(Collections::emptyList);
		this.appRedisHostList = appRedisHostListObjectProvider.getIfAvailable() ;
	}

	@Bean(destroyMethod = "shutdown")
	public DefaultClientResources lettuceClientResources() {
		//重连延迟时间固定时间
		DefaultClientResources clientResources = DefaultClientResources.builder()
				.reconnectDelay(Delay.constant( Duration.ofSeconds( 10 ) ) )
				.build() ;

		return clientResources ;
	}

	@Bean
	public RedisConnectionFactory redisConnectionFactory(
			ClientResources clientResources) throws UnknownHostException {
		LettuceClientConfiguration clientConfig = getLettuceClientConfiguration(
				clientResources, this.properties.getLettuce().getPool()) ;

		RedisConnectionFactory lettuceConnectionFactory = createLettuceConnectionFactory(clientConfig) ;

		return lettuceConnectionFactory ;
	}

	private RedisConnectionFactory createLettuceConnectionFactory(
			LettuceClientConfiguration clientConfiguration) {
		if (getSentinelConfig() != null) {
			throw new IllegalArgumentException("不支持 redis sentinel 配置") ;
		}
		if (getClusterConfiguration() != null) {
			throw new IllegalArgumentException("不支持 redis cluster 配置") ;
		}

		RedisConnectionFactory redisConnectionFactory = null ;

		if( properties.getHost().indexOf(",") != -1 ){
			redisConnectionFactory = new AppRedisDynamicConnectionFactory( properties ,
					clientConfiguration ,
					appRedisHostList ) ;
		}else{
			redisConnectionFactory = new AppCodisConnectionFactory( getStandaloneConfig(),
					clientConfiguration) ;
		}

		return redisConnectionFactory ;
	}

	private LettuceClientConfiguration getLettuceClientConfiguration(
			ClientResources clientResources, RedisProperties.Pool pool) {
		LettuceClientConfigurationBuilder builder = createBuilder(pool);
		applyProperties(builder);
		builder.clientResources(clientResources);
		customize(builder);
		return builder.build();
	}

	private LettuceClientConfigurationBuilder createBuilder(RedisProperties.Pool pool) {
		if (pool == null) {
			return LettuceClientConfiguration.builder();
		}
		return new PoolBuilderFactory().createBuilder(pool);
	}

	private LettuceClientConfigurationBuilder applyProperties(
			LettuceClientConfigurationBuilder builder) {
		if (this.properties.isSsl()) {
			builder.useSsl();
		}

		// 默认连接超时时间
		if( this.properties.getTimeout()==null ){
			this.properties.setTimeout( Duration.ofSeconds( DEFAULT_CONNECTION_TIMEOUT ) );
		}

		builder.commandTimeout(this.properties.getTimeout());

		if (this.properties.getLettuce() != null) {
			RedisProperties.Lettuce lettuce = this.properties.getLettuce();
			if (lettuce.getShutdownTimeout() != null
					&& !lettuce.getShutdownTimeout().isZero()) {
				builder.shutdownTimeout(
						this.properties.getLettuce().getShutdownTimeout());
			}
		}
		return builder;
	}

	private void customize(
			LettuceClientConfigurationBuilder builder) {
		for (LettuceClientConfigurationBuilderCustomizer customizer : this.builderCustomizers) {
			customizer.customize(builder);
		}
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

	/**
	 * Inner class to allow optional commons-pool2 dependency.
	 */
	private static class PoolBuilderFactory {

		public LettuceClientConfigurationBuilder createBuilder(RedisProperties.Pool properties) {
			return LettucePoolingClientConfiguration.builder()
					.poolConfig(getPoolConfig(properties)) ;
		}

		private GenericObjectPoolConfig getPoolConfig(RedisProperties.Pool properties) {
			GenericObjectPoolConfig config = new GenericObjectPoolConfig();
			config.setMaxTotal(properties.getMaxActive());
			config.setMaxIdle(properties.getMaxIdle());
			config.setMinIdle(properties.getMinIdle());
			if (properties.getMaxWait() != null) {
				config.setMaxWaitMillis(properties.getMaxWait().toMillis());
			}
			return config;
		}

	}

}
