package com.echat.storm.analysis.types;

import java.io.Serializable;

public class RedisConfig implements Serializable {
	public String host = "localhost";
	public int port = 3118;

	public static class Builder {
		private RedisConfig _conf = new RedisConfig();

		public RedisConfig build() {
			return _conf;
		}

		public Builder setHost(String host) {
			_conf.host = host;
			return this;
		}
		public Builder setPort(int port) {
			_conf.port = port;
			return this;
		}
	}
}

