package com.rtf.redis.client;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.data.redis.connection.*;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于redis连接的适配器
 * @Author : liupeng
 * @Date : 2019-03-27
 * @Modified By
 */
@Setter
@Getter
public abstract class AppRedisConnectionConfigurationAdaptor {

	private final RedisProperties properties;

	private RedisSentinelConfiguration sentinelConfiguration;

	private RedisClusterConfiguration clusterConfiguration;

	protected AppRedisConnectionConfigurationAdaptor(RedisProperties properties,
													 ObjectProvider<RedisSentinelConfiguration> sentinelConfigurationProvider,
													 ObjectProvider<RedisClusterConfiguration> clusterConfigurationProvider) {
		this.properties = properties;
		this.sentinelConfiguration = sentinelConfigurationProvider.getIfAvailable();
		this.clusterConfiguration = clusterConfigurationProvider.getIfAvailable();
	}

	protected AppRedisConnectionConfigurationAdaptor(RedisProperties properties) {
		this.properties = properties;
	}

	protected final RedisStandaloneConfiguration getStandaloneConfig() {
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
		if (StringUtils.hasText(this.properties.getUrl())) {
			ConnectionInfo connectionInfo = parseUrl(this.properties.getUrl());
			config.setHostName(connectionInfo.getHostName());
			config.setPort(connectionInfo.getPort());
			config.setPassword(RedisPassword.of(connectionInfo.getPassword()));
		}
		else {
			config.setHostName(this.properties.getHost());
			config.setPort(this.properties.getPort());
			config.setPassword(RedisPassword.of(this.properties.getPassword()));
		}
		config.setDatabase(this.properties.getDatabase());
		return config;
	}

	protected final RedisSentinelConfiguration getSentinelConfig() {
		if (this.sentinelConfiguration != null) {
			return this.sentinelConfiguration;
		}
		RedisProperties.Sentinel sentinelProperties = this.properties.getSentinel();
		if (sentinelProperties != null) {
			RedisSentinelConfiguration config = new RedisSentinelConfiguration();
			config.master(sentinelProperties.getMaster());
			config.setSentinels(createSentinels(sentinelProperties));
			if (this.properties.getPassword() != null) {
				config.setPassword(RedisPassword.of(this.properties.getPassword()));
			}
			config.setDatabase(this.properties.getDatabase());
			return config;
		}
		return null;
	}

	/**
	 * Create a {@link RedisClusterConfiguration} if necessary.
	 * @return {@literal null} if no cluster settings are set.
	 */
	protected final RedisClusterConfiguration getClusterConfiguration() {
		if (this.clusterConfiguration != null) {
			return this.clusterConfiguration;
		}
		if (this.properties.getCluster() == null) {
			return null;
		}
		RedisProperties.Cluster clusterProperties = this.properties.getCluster();
		RedisClusterConfiguration config = new RedisClusterConfiguration(
				clusterProperties.getNodes());
		if (clusterProperties.getMaxRedirects() != null) {
			config.setMaxRedirects(clusterProperties.getMaxRedirects());
		}
		if (this.properties.getPassword() != null) {
			config.setPassword(RedisPassword.of(this.properties.getPassword()));
		}
		return config;
	}

	private List<RedisNode> createSentinels(RedisProperties.Sentinel sentinel) {
		List<RedisNode> nodes = new ArrayList<>();
		for (String node : sentinel.getNodes()) {
			try {
				String[] parts = StringUtils.split(node, ":");
				Assert.state(parts.length == 2, "Must be defined as 'host:port'");
				nodes.add(new RedisNode(parts[0], Integer.valueOf(parts[1])));
			}
			catch (RuntimeException ex) {
				throw new IllegalStateException(
						"Invalid redis sentinel " + "property '" + node + "'", ex);
			}
		}
		return nodes;
	}

	protected ConnectionInfo parseUrl(String url) {
		try {
			URI uri = new URI(url);
			boolean useSsl = (url.startsWith("rediss://"));
			String password = null;
			if (uri.getUserInfo() != null) {
				password = uri.getUserInfo();
				int index = password.indexOf(':');
				if (index >= 0) {
					password = password.substring(index + 1);
				}
			}
			return new ConnectionInfo(uri, useSsl, password);
		}
		catch (URISyntaxException ex) {
			throw new IllegalArgumentException("Malformed url '" + url + "'", ex);
		}
	}

	protected static class ConnectionInfo {

		private final URI uri;

		private final boolean useSsl;

		private final String password;

		public ConnectionInfo(URI uri, boolean useSsl, String password) {
			this.uri = uri;
			this.useSsl = useSsl;
			this.password = password;
		}

		public boolean isUseSsl() {
			return this.useSsl;
		}

		public String getHostName() {
			return this.uri.getHost();
		}

		public int getPort() {
			return this.uri.getPort();
		}

		public String getPassword() {
			return this.password;
		}

	}

}
