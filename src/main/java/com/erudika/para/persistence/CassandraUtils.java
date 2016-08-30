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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.erudika.para.utils.Config;


import javax.inject.Singleton;

/**
 * Apache Cassandra DAO utilities for Para.
 * @author Luca Venturella [lucaventurella@gmail.com]
 */
@Singleton
public final class CassandraUtils {

	private static final Logger logger = LoggerFactory.getLogger(CassandraUtils.class);
//	private static CassandraClient cassandraClient;
	private static Session cassandra;
	private static Cluster cluster;
	private static final String DBHOSTS = Config.getConfigParam("cassandra.hosts", "localhost");
	private static final int DBPORT = Config.getConfigInt("cassandra.port", 9042);
	private static final String DBNAME = Config.getConfigParam("cassandra.keyspace", Config.APP_NAME_NS);
	private static final String DBUSER = Config.getConfigParam("cassandra.user", "");
	private static final String DBPASS = Config.getConfigParam("cassandra.password", "");
	private static final int REPLICATION = Config.getConfigInt("cassandra.replication_factor", 1);

	//////////  DB CONFIG  //////////////
	public static final String CLUSTER = Config.CLUSTER_NAME;
	public static final int CASSANDRA_PORT = 9160;
	////////////////////////////////////

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
//			ResultSet rs = session.execute("select release_version from system.local");
//			Row row = rs.one();
			logger.debug("Cassandra host: " + DBHOSTS + ":" + DBPORT + ", keyspace: " + DBNAME);
		} catch (Exception e) {
			logger.error("Failed to connect ot Cassandra: {}.", e.getMessage());
		}
//
//		ServerAddress s = new ServerAddress(DBHOST, DBPORT);
//		if (!StringUtils.isBlank(DBUSER) && !StringUtils.isBlank(DBPASS)) {
//			CassandraCredential credential = CassandraCredential.createCredential(DBUSER, DBNAME, DBPASS.toCharArray());
//			cassandraClient = new CassandraClient(s, Arrays.asList(credential));
//		} else {
//			cassandraClient = new CassandraClient(s);
//		}
//
//		cassandra = cassandraClient.getDatabase(DBNAME);
//

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
			appid = getTableNameForAppid(appid);
			ResultSet res = getClient().execute("SELECT id FROM " + DBNAME + " LIMIT 0;");
			res.one();
		    return true;
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

//	/**
//	 * Gives count information about a Cassandra table.
//	 * @param appid name of the collection
//	 * @return a long
//	 */
//	public static long getTableCount(final String appid) {
//		if (StringUtils.isBlank(appid)) {
//			return -1;
//		}
//		try {
//			Row row = getClient().execute("SELECT __count FROM " + getTableNameForAppid(appid) + ";").
//					one();
//			if (row != null) {
//				Map<String, Object> val = ParaObjectUtils.getJsonReader(Map.class).readValue(row.getString("json"));
//				return val.get("value") == null ? 0L : (Long) val.get("value");
//			}
//		} catch (Exception e) {
//			logger.error(null, e);
//		}
//		return -1;
//	}
//
//	/**
//	 * Get the cassandra table requested
//	 * @param appid name of the collection
//	 * @return a Cassandra collection
//	 */
//	public static CassandraCollection<Document> getTable(String appid) {
//		try {
//			return getClient().getCollection();
//		} catch (Exception e) {
//			logger.error(null, e);
//		}
//		return null;
//	}

//	/**
//	 * Lists all table names for this account.
//	 * @return a list of Cassandra tables
//	 */
//	public static CassandraIterable<String> listAllTables() {
//		CassandraIterable<String> collectionNames = getClient().listCollectionNames();
//		return collectionNames;
//	}

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

//	/**
//	 * Create a new unique objectid for Cassandra
//	 * @return the objectid as string
//	 */
//	public static String generateNewId(){
//		return new ObjectId().toHexString();
//	}
}
