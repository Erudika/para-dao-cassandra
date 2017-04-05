/*
 * Copyright 2013-2017 Erudika. https://erudika.com
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

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.erudika.para.annotations.Locked;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.utils.ParaObjectUtils;
import static com.erudika.para.persistence.CassandraUtils.getClient;
import static com.erudika.para.persistence.CassandraUtils.getPreparedStatement;
import com.erudika.para.utils.Config;
import com.erudika.para.utils.Pager;
import com.erudika.para.utils.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Apache Cassandra DAO implementation for Para.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
@Singleton
public class CassandraDAO implements DAO {

	private static final Logger logger = LoggerFactory.getLogger(CassandraDAO.class);
	private static final Map<String, PreparedStatement> statements = new HashMap<String, PreparedStatement>();

	public CassandraDAO() { }

	/////////////////////////////////////////////
	//			CORE FUNCTIONS
	/////////////////////////////////////////////

	@Override
	public <P extends ParaObject> String create(String appid, P so) {
		if (so == null) {
			return null;
		}
		if (!StringUtils.contains(so.getId(), Config.SEPARATOR)) {
			if (StringUtils.isBlank(so.getId())) {
				so.setId(Utils.getNewId());
				logger.debug("Generated new id: " + so.getId());
			}
		}
		if (so.getTimestamp() == null) {
			so.setTimestamp(Utils.timestamp());
		}
		so.setAppid(appid);
		createRow(so.getId(), appid, toRow(so, null));
		logger.debug("DAO.create() {}", so.getId());
		return so.getId();
	}

	@Override
	public <P extends ParaObject> P read(String appid, String key) {
		if (StringUtils.isBlank(key)) {
			return null;
		}
		P so = fromRow(readRow(key, appid));
		logger.debug("DAO.read() {} -> {}", key, so == null ? null : so.getType());
		return so != null ? so : null;
	}

	@Override
	public <P extends ParaObject> void update(String appid, P so) {
		if (so != null && so.getId() != null) {
			so.setUpdated(Utils.timestamp());
			updateRow(so, appid);
			logger.debug("DAO.update() {}", so.getId());
		}
	}

	@Override
	public <P extends ParaObject> void delete(String appid, P so) {
		if (so != null && so.getId() != null) {
			deleteRow(so.getId(), appid);
			logger.debug("DAO.delete() {}", so.getId());
		}
	}

	/////////////////////////////////////////////
	//				ROW FUNCTIONS
	/////////////////////////////////////////////

	private String createRow(String key, String appid, String row) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(appid) || row == null || row.isEmpty()) {
			return null;
		}
		try {
			// if there isn't a document with the same id then create a new document
			// else replace the document with the same id with the new one
			PreparedStatement ps = getPreparedStatement("INSERT INTO " +
					CassandraUtils.getTableNameForAppid(appid) +" (id, json) VALUES (?, ?);");
			getClient().execute(ps.bind(key, row));
			logger.debug("Created id: " + key + " row: " + row);
		} catch (Exception e) {
			logger.error(null, e);
		}
		return key;
	}

	private <P extends ParaObject> void updateRow(P so, String appid) {
		if (so == null || so.getId() == null || StringUtils.isBlank(appid)) {
			return;
		}
		try {
			String oldRow = readRow(so.getId(), appid);
			if (oldRow != null) {
				Map<String, Object> oldData = ParaObjectUtils.getJsonReader(Map.class).readValue(oldRow);
				Map<String, Object> newData = ParaObjectUtils.getAnnotatedFields(so, Locked.class);
				oldData.putAll(newData);
				PreparedStatement ps = getPreparedStatement("UPDATE " +
						CassandraUtils.getTableNameForAppid(appid) + " SET json = ? WHERE id = ?;");
				getClient().execute(ps.bind(ParaObjectUtils.getJsonWriterNoIdent().
						writeValueAsString(oldData), so.getId()));
				logger.debug("Updated id: " + so.getId());
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
	}

	private String readRow(String key, String appid) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(appid)) {
			return null;
		}
		String row = null;
		try {
			PreparedStatement ps = getPreparedStatement("SELECT json FROM " +
					CassandraUtils.getTableNameForAppid(appid) + " WHERE id = ?;");
			Row r = getClient().execute(ps.bind(key)).one();
			if (r != null) {
				row = r.getString("json");
			}
			logger.debug("Read id: " + key + " row: " + row);
		} catch (Exception e) {
			logger.error(null, e);
		}
		return (row == null || row.isEmpty()) ? null : row;
	}

	private void deleteRow(String key, String appid) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(appid)) {
			return;
		}
		try {
			PreparedStatement ps = getPreparedStatement("DELETE FROM " +
					CassandraUtils.getTableNameForAppid(appid) + " WHERE id = ?;");
			getClient().execute(ps.bind(key));
			logger.debug("Deleted id: " + key);
		} catch (Exception e) {
			logger.error(null, e);
		}
	}

	/////////////////////////////////////////////
	//				READ ALL FUNCTIONS
	/////////////////////////////////////////////

	@Override
	public <P extends ParaObject> void createAll(String appid, List<P> objects) {
		if (objects == null || objects.isEmpty() || StringUtils.isBlank(appid)) {
			return;
		}
		ArrayList<String> values = new ArrayList<String>(objects.size());
		StringBuilder batch = new StringBuilder("BEGIN BATCH ");
		for (ParaObject so : objects) {
			if (so != null) {
				if (StringUtils.isBlank(so.getId())) {
					so.setId(Utils.getNewId());
					logger.debug("Generated id: " + so.getId());
				}
				if (so.getTimestamp() == null) {
					so.setTimestamp(Utils.timestamp());
				}
				so.setAppid(appid);
				batch.append("INSERT INTO ").append(CassandraUtils.getTableNameForAppid(appid)).
						append(" (id, json) VALUES (?, ?);");
				values.add(so.getId());
				values.add(toRow(so, null));
			}
		}

		if (!values.isEmpty()) {
			batch.append("APPLY BATCH");
			PreparedStatement ps = getClient().prepare(batch.toString());
			getClient().execute(ps.bind(values.toArray()));
		}
		logger.debug("DAO.createAll() {}", objects.size());
	}

	@Override
	public <P extends ParaObject> Map<String, P> readAll(String appid, List<String> keys, boolean getAllColumns) {
		if (keys == null || keys.isEmpty() || StringUtils.isBlank(appid)) {
			return new LinkedHashMap<String, P>();
		}
		Map<String, P> results = new LinkedHashMap<String, P>(keys.size(), 0.75f, true);
		PreparedStatement ps = getPreparedStatement("SELECT id, json FROM " +
				CassandraUtils.getTableNameForAppid(appid) + " WHERE id = ?;");

		List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>(keys.size());
		for (String key : keys) {
			ResultSetFuture resultSetFuture = getClient().executeAsync(ps.bind(key));
			futures.add(resultSetFuture);
		}
		for (ResultSetFuture future : futures) {
			ResultSet rows = future.getUninterruptibly();
			Row row = rows.one();
			String json = row.getString("json");
			if (!StringUtils.isBlank(json)) {
				P obj = fromRow(json);
				results.put(row.getString("id"), obj);
			}
		}
		logger.debug("DAO.readAll() {}", results.size());
		return results;
	}

	@Override
	public <P extends ParaObject> List<P> readPage(String appid, Pager pager) {
		LinkedList<P> results = new LinkedList<P>();
		if (StringUtils.isBlank(appid)) {
			return results;
		}
		if (pager == null) {
			pager = new Pager();
		}
		try {
			Statement st = new SimpleStatement("SELECT json FROM " + CassandraUtils.getTableNameForAppid(appid) + ";");
			st.setFetchSize(pager.getLimit());
			String lastPage = pager.getLastKey();
			if (lastPage != null) {
				if ("end".equals(lastPage)) {
					return results;
				} else {
					st.setPagingState(PagingState.fromString(lastPage));
				}
			}
			ResultSet rs = getClient().execute(st);
			PagingState nextPage = rs.getExecutionInfo().getPagingState();

			int remaining = rs.getAvailableWithoutFetching();
			for (Row row : rs) {
				P obj = fromRow(row.getString("json"));
				if (obj != null) {
					results.add(obj);
				}
				if (--remaining == 0) {
					break;
				}
			}

			if (nextPage != null) {
				pager.setLastKey(nextPage.toString());
			} else {
				pager.setLastKey("end");
			}

			if (!results.isEmpty()) {
				pager.setCount(pager.getCount() + results.size());
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
		logger.debug("readPage() page: {}, results:", pager.getPage(), results.size());
		return results;
	}

	@Override
	public <P extends ParaObject> void updateAll(String appid, List<P> objects) {
		if (StringUtils.isBlank(appid) || objects == null) {
			return;
		}
		try {
			ArrayList<String> keys = new ArrayList<String>(objects.size());
			for (P obj : objects) {
				if (obj != null) {
					keys.add(obj.getId());
				}
			}
			// we read all existing rows first then merge the new data with existing data
			Map<String, P> existing = readAll(appid, keys, true);
			ArrayList<String> values = new ArrayList<String>(objects.size());
			StringBuilder batch = new StringBuilder("BEGIN BATCH ");
			for (P newObj : objects) {
				if (newObj != null) {
					P oldObj = existing.get(newObj.getId());
					if (oldObj != null) {
						Map<String, Object> oldData =
								new HashMap<String, Object>(ParaObjectUtils.getAnnotatedFields(oldObj, null));
						Map<String, Object> newData = ParaObjectUtils.getAnnotatedFields(newObj, Locked.class);
						oldData.putAll(newData);

						long now = Utils.timestamp();
						newObj.setUpdated(now);
						oldData.put(Config._UPDATED, now);
						oldData.put(Config._APPID, appid);
						batch.append("UPDATE ").append(CassandraUtils.getTableNameForAppid(appid)).
								append(" SET json = ? WHERE id = ?;");
						values.add(ParaObjectUtils.getJsonWriterNoIdent().writeValueAsString(oldData));
						values.add(newObj.getId());
					}
				}
			}
			if (!values.isEmpty()) {
				batch.append("APPLY BATCH");
				PreparedStatement ps = getClient().prepare(batch.toString());
				getClient().execute(ps.bind(values.toArray()));
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
		logger.debug("DAO.updateAll() {}", objects.size());
	}

	@Override
	public <P extends ParaObject> void deleteAll(String appid, List<P> objects) {
		if (objects == null || objects.isEmpty() || StringUtils.isBlank(appid)) {
			return;
		}
		try {
			ArrayList<String> values = new ArrayList<String>(objects.size());
			StringBuilder batch = new StringBuilder("BEGIN BATCH ");
			for (ParaObject so : objects) {
				if (so != null) {
					so.setAppid(appid);
					batch.append("DELETE FROM ").append(CassandraUtils.getTableNameForAppid(appid)).
							append(" WHERE id = ?;");
					values.add(so.getId());
				}
			}

			if (!values.isEmpty()) {
				batch.append("APPLY BATCH");
				PreparedStatement ps = getClient().prepare(batch.toString());
				getClient().execute(ps.bind(values.toArray()));
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
		logger.debug("DAO.deleteAll() {}", objects.size());
	}

	/////////////////////////////////////////////
	//				MISC FUNCTIONS
	/////////////////////////////////////////////

	private <P extends ParaObject> String toRow(P so, Class<? extends Annotation> filter) {
		String row = null;
		if (so == null) {
			return row;
		}
		try {
			row = ParaObjectUtils.getJsonWriterNoIdent().
					writeValueAsString(ParaObjectUtils.getAnnotatedFields(so, filter));
		} catch (JsonProcessingException ex) {
			logger.error(null, ex);
		}
		return row;
	}

	private <P extends ParaObject> P fromRow(String row) {
		if (row == null || row.isEmpty()) {
			logger.debug("row is null or empty");
			return null;
		}
		Map<String, Object> props = new HashMap<String, Object>();
		try {
			props = ParaObjectUtils.getJsonReader(Map.class).readValue(row);
		} catch (IOException ex) {
			logger.error(null, ex);
		}
		return ParaObjectUtils.setAnnotatedFields(props);
	}

	//////////////////////////////////////////////////////

	@Override
	public <P extends ParaObject> String create(P so) {
		return create(Config.APP_NAME_NS, so);
	}

	@Override
	public <P extends ParaObject> P read(String key) {
		return read(Config.APP_NAME_NS, key);
	}

	@Override
	public <P extends ParaObject> void update(P so) {
		update(Config.APP_NAME_NS, so);
	}

	@Override
	public <P extends ParaObject> void delete(P so) {
		delete(Config.APP_NAME_NS, so);
	}

	@Override
	public <P extends ParaObject> void createAll(List<P> objects) {
		createAll(Config.APP_NAME_NS, objects);
	}

	@Override
	public <P extends ParaObject> Map<String, P> readAll(List<String> keys, boolean getAllColumns) {
		return readAll(Config.APP_NAME_NS, keys, getAllColumns);
	}

	@Override
	public <P extends ParaObject> List<P> readPage(Pager pager) {
		return readPage(Config.APP_NAME_NS, pager);
	}

	@Override
	public <P extends ParaObject> void updateAll(List<P> objects) {
		updateAll(Config.APP_NAME_NS, objects);
	}

	@Override
	public <P extends ParaObject> void deleteAll(List<P> objects) {
		deleteAll(Config.APP_NAME_NS, objects);
	}

}
