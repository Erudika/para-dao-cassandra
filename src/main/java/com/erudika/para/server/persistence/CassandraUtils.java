/*
 * Copyright 2013-2022 Erudika. https://erudika.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For issues and patches go to: https://github.com/erudika
 */
package com.erudika.para.server.persistence;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.erudika.para.core.App;
import com.erudika.para.core.listeners.DestroyListener;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Para;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import nl.altindag.ssl.SSLFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Cassandra DAO utilities for Para.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public final class CassandraUtils {

	private static final Logger logger = LoggerFactory.getLogger(CassandraUtils.class);
	private static CqlSession session;
	private static final String DBHOSTS = Para.getConfig().cassandraHosts();
	private static final int DBPORT = Para.getConfig().cassandraPort();
	private static final String DBNAME = Para.getConfig().cassandraKeyspace();
	private static final String DBUSER = Para.getConfig().cassandraUser();
	private static final String DBPASS = Para.getConfig().cassandraPassword();
	private static final int REPLICATION = Para.getConfig().cassandraReplicationFactor();
	private static final boolean SSL = Para.getConfig().cassandraSslEnabled();
	private static final String PROTOCOLS = Para.getConfig().cassandraSslProtocols();
	private static final String KEYSTORE_PATH = Para.getConfig().cassandraSslKeystore();
	private static final String KEYSTORE_PASS = Para.getConfig().cassandraSslKeystorePassword();
	private static final String TRUSTSTORE_PATH = Para.getConfig().cassandraTruststore();
	private static final String TRUSTSTORE_PASS = Para.getConfig().cassandraTruststorePassword();

	private static final Map<String, PreparedStatement> STATEMENTS = new ConcurrentHashMap<String, PreparedStatement>();

	static {
		// Fix for exceptions from Spring Boot when using a different MongoDB host than localhost.
		System.setProperty("spring.autoconfigure.exclude",
				"org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration,"
						+ "org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration");
	}

	private CassandraUtils() { }

	/**
	 * Returns a Cassandra session object.
	 * @return a connection session to Cassandra
	 */
	public static CqlSession getClient() {
		if (session != null) {
			return session;
		}
		try {
			SSLFactory sslFactory = null;
			if (SSL) {
				if (!StringUtils.isBlank(TRUSTSTORE_PATH)) {
					sslFactory = SSLFactory.builder()
							.withTrustMaterial(Paths.get(TRUSTSTORE_PATH), TRUSTSTORE_PASS.toCharArray())
							.withProtocols(PROTOCOLS).build();
				}
				if (!StringUtils.isBlank(KEYSTORE_PATH)) {
					sslFactory = SSLFactory.builder()
							.withIdentityMaterial(Paths.get(KEYSTORE_PATH), KEYSTORE_PASS.toCharArray())
							.withTrustMaterial(Paths.get(TRUSTSTORE_PATH), TRUSTSTORE_PASS.toCharArray())
							.withProtocols(PROTOCOLS).build();
				}
				if (sslFactory == null) {
					sslFactory = SSLFactory.builder().withDefaultTrustMaterial().build();
				}
			}
			session = CqlSession.builder().addContactPoints(Arrays.asList(DBHOSTS.split(",")).stream().
					map(e -> InetSocketAddress.createUnresolved(e, DBPORT)).collect(Collectors.toList())).
					withSslContext(sslFactory == null ?  null : sslFactory.getSslContext()).
					withAuthCredentials(DBUSER, DBPASS).build();
			if (!existsTable(Para.getConfig().getRootAppIdentifier())) {
				createTable(session, Para.getConfig().getRootAppIdentifier());
			} else {
				session.execute("USE " + DBNAME + ";");
			}
			logger.debug("Cassandra host: " + DBHOSTS + ":" + DBPORT + ", keyspace: " + DBNAME);
		} catch (Exception e) {
			logger.error("Failed to connect ot Cassandra: {}.", e.getMessage());
		}

		Para.addDestroyListener(new DestroyListener() {
			public void onDestroy() {
				shutdownClient();
			}
		});

		return session;
	}

	/**
	 * Stops the client and releases resources.
	 * You can tell Para to call this on shutdown using {@code Para.addDestroyListener()}
	 */
	public static void shutdownClient() {
		if (session != null) {
			session.close();
			session = null;
		}
	}

	/**
	 * Checks if the main table exists in the database.
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @return true if the table exists
	 */
	public static boolean existsTable(String appid) {
		if (StringUtils.isBlank(appid)) {
			return false;
		}
		if (session == null) {
			throw new IllegalStateException("Cassandra client not initialized.");
		}
		try {
			KeyspaceMetadata ks = session.getMetadata().getKeyspace(DBNAME).get();
			TableMetadata table = ks.getTable(getTableNameForAppid(appid)).get();
			return table != null && table.getName() != null;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Creates a table in Cassandra.
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @return true if created
	 */
	public static boolean createTable(String appid) {
		return createTable(getClient(), appid);
	}

	static boolean createTable(Session client, String appid) {
		if (StringUtils.isBlank(appid) || StringUtils.containsWhitespace(appid) ||
				existsTable(appid) || client == null) {
			return false;
		}
		String table = getTableNameForAppid(appid);
		try {
			if (session.getMetadata().getKeyspace(DBNAME).isEmpty()) {
				session.execute("CREATE KEYSPACE IF NOT EXISTS " + DBNAME +
						" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + REPLICATION + "};");
			}
		} catch (Exception e) {
			logger.warn("Could not create keyspace {}!", DBNAME);
		}
		try {
			session.execute("USE " + DBNAME + ";");
			session.execute("CREATE TABLE IF NOT EXISTS " + table + " (id text PRIMARY KEY, json text, json_updates text);");
			logger.info("Created Cassandra table '{}'.", table);
		} catch (Exception e) {
			logger.error(null, e);
			return false;
		}
		return true;
	}

	/**
	 * Deletes the main table from Cassandra.
	 * @param appid name of the {@link com.erudika.para.core.App}
	 * @return true if deleted
	 */
	public static boolean deleteTable(String appid) {
		if (StringUtils.isBlank(appid) || !existsTable(appid)) {
			return false;
		}
		try {
			String table = getTableNameForAppid(appid);
			getClient().execute("DROP TABLE IF EXISTS " + table + ";");
			logger.info("Deleted Cassandra table '{}'.", table);
		} catch (Exception e) {
			logger.error(null, e);
			return false;
		}
		return false;
	}

	/**
	 * Returns the table name for a given app id. Table names are usually in the form 'prefix_appid'.
	 * @param appIdentifier app id
	 * @return the table name
	 */
	public static String getTableNameForAppid(String appIdentifier) {
		if (StringUtils.isBlank(appIdentifier)) {
			return null;
		} else {
			return (App.isRoot(appIdentifier) || appIdentifier.startsWith(Config.PARA.concat("-")) ?
					appIdentifier : Config.PARA + "-" + appIdentifier).replaceAll("-", "_");
		}
	}

	/**
	 * Caches the prepared statements on the query (key).
	 * @param query a CQL query
	 * @return a prepared statement
	 */
	protected static synchronized PreparedStatement getPreparedStatement(String query) {
		if (STATEMENTS.containsKey(query)) {
			return STATEMENTS.get(query);
		} else {
			PreparedStatement ps = getClient().prepare(query);
			STATEMENTS.put(query, ps);
			return ps;
		}
	}
}
