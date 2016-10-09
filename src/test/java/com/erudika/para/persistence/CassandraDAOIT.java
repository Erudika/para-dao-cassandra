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

import com.erudika.para.utils.Config;
import java.io.IOException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class CassandraDAOIT extends DAOTest {

	@BeforeClass
	public static void setUpClass() throws InterruptedException, TTransportException, IOException {
		System.setProperty("para.cassandra.port", "9142");
		EmbeddedCassandraServerHelper.startEmbeddedCassandra(15 * 1000);
		dao = new CassandraDAO();
		CassandraUtils.createTable(Config.APP_NAME_NS);
		CassandraUtils.createTable(appid1);
		CassandraUtils.createTable(appid2);
		CassandraUtils.createTable(appid3);
	}

	@AfterClass
	public static void tearDownClass() {
		CassandraUtils.deleteTable(Config.APP_NAME_NS);
		CassandraUtils.deleteTable(appid1);
		CassandraUtils.deleteTable(appid2);
		CassandraUtils.deleteTable(appid3);
		CassandraUtils.shutdownClient();
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	@Test
	public void testCreateDeleteExistsTable() throws InterruptedException {
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

}
