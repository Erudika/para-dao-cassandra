/*
 * Copyright 2013-2026 Erudika. https://erudika.com
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
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
@Testcontainers(disabledWithoutDocker = true)
public class CassandraDAOIT extends DAOTest {

	private static final String ROOT_APP_NAME = "para-test";
	private static final int MAX_CONNECT_ATTEMPTS = 60;
	private static final long RETRY_DELAY_MS = 10000L;
	private static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse("cassandra:4.1.10");
	@Container
	private static final GenericContainer<?> CASSANDRA = new GenericContainer<>(CASSANDRA_IMAGE)
//			.withEnv("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator")
//			.withEnv("CASSANDRA_AUTHORIZER", "CassandraAuthorizer")
//			.withEnv("CASSANDRA_ROLE_MANAGER", "CassandraRoleManager")
			.withExposedPorts(9042);
//			.waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
	private static final Logger logger = LoggerFactory.getLogger(CassandraDAOIT.class.getName());

	public CassandraDAOIT() {
		super(new CassandraDAO());
	}

	@BeforeAll
	public static void setUpClass() throws InterruptedException {
		System.setProperty("api.version", "1.44");
		System.setProperty("para.cassandra.hosts", CASSANDRA.getHost());
		System.setProperty("para.cassandra.port", String.valueOf(CASSANDRA.getMappedPort(9042)));
		System.setProperty("para.cassandra.user", "cassandra");
		System.setProperty("para.cassandra.password", "cassandra");
		System.setProperty("para.app_name", ROOT_APP_NAME);
		System.setProperty("para.cluster_name", ROOT_APP_NAME);
		waitForCassandra();
		CassandraUtils.createTable(ROOT_APP_NAME);
		CassandraUtils.createTable(appid1);
		CassandraUtils.createTable(appid2);
		CassandraUtils.createTable(appid3);
	}

	@AfterAll
	public static void tearDownClass() {
		CassandraUtils.deleteTable(ROOT_APP_NAME);
		CassandraUtils.deleteTable(appid1);
		CassandraUtils.deleteTable(appid2);
		CassandraUtils.deleteTable(appid3);
		CassandraUtils.shutdownClient();
	}

	@Test
	public void testCreateDeleteExistsTable() {
		String testappid1 = "test-index";
		String badAppid = "test index 123";

		CassandraUtils.createTable("");
		assertFalse(CassandraUtils.existsTable(""));

		CassandraUtils.createTable(testappid1);
		assertTrue(CassandraUtils.existsTable(testappid1));

		CassandraUtils.deleteTable(testappid1);
		assertFalse(CassandraUtils.existsTable(testappid1));

		assertFalse(CassandraUtils.createTable(badAppid));
		assertFalse(CassandraUtils.existsTable(badAppid));
		assertFalse(CassandraUtils.deleteTable(badAppid));
	}

	private static void waitForCassandra() throws InterruptedException {
//		for (int i = 0; i < MAX_CONNECT_ATTEMPTS; i++) {
			Thread.sleep(RETRY_DELAY_MS);
			try {
				CqlSession client = CassandraUtils.getClient();
				if (client != null) {
					return;
				}
			} catch (IllegalStateException e) {
				logger.warn("Waiting for Cassandra to become available: {}", e.getMessage());
			}
//		}
		fail("Timed out waiting for Cassandra to start.");
	}

}
