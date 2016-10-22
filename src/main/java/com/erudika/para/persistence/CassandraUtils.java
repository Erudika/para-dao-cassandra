/*
 * Copyright 2013-2016 Erudika. http://erudika.com
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
package com.erudika.para.persistence;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.erudika.para.utils.Config;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


import javax.inject.Singleton;

/**
 * Apache Cassandra DAO utilities for Para.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
@Singleton
public final class CassandraUtils {

	private static final Logger logger = LoggerFactory.getLogger(CassandraUtils.class);
	private static Session cassandra;
	private static Cluster cluster;
	private static final String DBHOSTS = Config.getConfigParam("cassandra.hosts", "localhost");
	private static final int DBPORT = Config.getConfigInt("cassandra.port", 9042);
	private static final String DBNAME = getTableNameForAppid(Config.getConfigParam("cassandra.keyspace", Config.APP_NAME_NS));
	private static final String DBUSER = Config.getConfigParam("cassandra.user", "");
	private static final String DBPASS = Config.getConfigParam("cassandra.password", "");
	private static final int REPLICATION = Config.getConfigInt("cassandra.replication_factor", 1);
	private static final Map<String, PreparedStatement> statements = new ConcurrentHashMap<String, PreparedStatement>();

	private CassandraUtils() { }

	/**
	 * Returns a Cassandra session object
	 * @return a connection session to Cassandra
	 */
	public static Session getClient() {
		if (cassandra != null) {
			return cassandra;
		}
		try {
			cluster = Cluster.builder().addContactPoints(DBHOSTS.split(",")).
					withPort(DBPORT).withCredentials(DBUSER, DBPASS).build();
			cassandra = cluster.connect();
			if (!existsTable(Config.APP_NAME_NS)) {
				createTable(Config.APP_NAME_NS);
			}
			logger.debug("Cassandra host: " + DBHOSTS + ":" + DBPORT + ", keyspace: " + DBNAME);
		} catch (Exception e) {
			logger.error("Failed to connect ot Cassandra: {}.", e.getMessage());
		}

		// We don't have access to Para.addDestroyListener() here.
		// Users will be responsible for calling shutDownClient().

		return cassandra;
	}

	/**
	 * Stops the client and releases resources.
	 * You can tell Para to call this on shutdown using {@code Para.addDestroyListener()}
	 */
	public static void shutdownClient() {
		if (cassandra != null) {
			cassandra.close();
			cassandra = null;
		}
		if (cluster != null) {
			cluster.close();
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
		try {
			getClient(); // just in case cluster var is null
			KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(DBNAME);
			TableMetadata table = ks.getTable(getTableNameForAppid(appid));
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
		if (StringUtils.isBlank(appid) || StringUtils.containsWhitespace(appid) || existsTable(appid)) {
			return false;
		}
		try {
			String table = getTableNameForAppid(appid);
			getClient().execute("CREATE KEYSPACE IF NOT EXISTS "+ DBNAME +
					" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + REPLICATION + "};");
			getClient().execute("USE " + DBNAME + ";");
			getClient().execute("CREATE TABLE IF NOT EXISTS " + table + " (id text PRIMARY KEY, json text);");
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
			getClient().execute("DROP TABLE IF EXISTS " + getTableNameForAppid(appid) + ";");
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
			return ((appIdentifier.equals(Config.APP_NAME_NS) || appIdentifier.startsWith(Config.PARA.concat("-"))) ?
					appIdentifier : Config.PARA + "-" + appIdentifier).replaceAll("-", "_");
		}
	}

	/**
	 * Caches the prepared statements on the query (key).
	 * @param query a CQL query
	 * @return a prepared statement
	 */
	protected synchronized static PreparedStatement getPreparedStatement(String query){
		if (statements.containsKey(query)) {
			return statements.get(query);
		} else {
			PreparedStatement ps = getClient().prepare(query);
			statements.put(query, ps);
			return ps;
		}
	}
}
