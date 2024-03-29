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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PagingState;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.annotations.Locked;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Pager;
import com.erudika.para.core.utils.Para;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.erudika.para.core.utils.Utils;
import static com.erudika.para.server.persistence.CassandraUtils.getClient;
import static com.erudika.para.server.persistence.CassandraUtils.getPreparedStatement;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Cassandra DAO implementation for Para.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class CassandraDAO implements DAO {

	private static final Logger logger = LoggerFactory.getLogger(CassandraDAO.class);

	static {
		// set up automatic table creation and deletion
		App.addAppCreatedListener((App app) -> {
			if (app != null && !app.isSharingTable()) {
				CassandraUtils.createTable(app.getAppIdentifier());
			}
		});
		App.addAppDeletedListener((App app) -> {
			if (app != null && !app.isSharingTable()) {
				CassandraUtils.deleteTable(app.getAppIdentifier());
			}
		});
	}

	/**
	 * Default constructor.
	 */
	public CassandraDAO() {
	}

	/////////////////////////////////////////////
	//			CORE FUNCTIONS
	/////////////////////////////////////////////

	@Override
	public <P extends ParaObject> String create(String appid, P so) {
		if (so == null) {
			return null;
		}
		if (!StringUtils.contains(so.getId(), Para.getConfig().separator())) {
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
		P so = readRow(key, appid);
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
					CassandraUtils.getTableNameForAppid(appid) + " (id, json, json_updates) VALUES (?, ?, NULL);");
			getClient().execute(ps.bind(key, row));
			logger.debug("Created id: " + key + " row: " + row);
		} catch (Exception e) {
			logger.error(null, e);
			throwIfNecessary(e);
		}
		return key;
	}

	private <P extends ParaObject> void updateRow(P so, String appid) {
		if (so == null || so.getId() == null || StringUtils.isBlank(appid)) {
			return;
		}
		try {
			Map<String, Object> data = ParaObjectUtils.getAnnotatedFields(so, Locked.class);
			PreparedStatement ps = getPreparedStatement("UPDATE " +
					CassandraUtils.getTableNameForAppid(appid) + " SET json_updates = ? WHERE id = ?;");
			getClient().execute(ps.bind(ParaObjectUtils.getJsonWriterNoIdent().writeValueAsString(data), so.getId()));
			logger.debug("Updated id: " + so.getId());
		} catch (Exception e) {
			logger.error(null, e);
			throwIfNecessary(e);
		}
	}

	private <P extends ParaObject> P readRow(String key, String appid) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(appid)) {
			return null;
		}
		try {
			PreparedStatement ps = getPreparedStatement("SELECT json, json_updates FROM " +
					CassandraUtils.getTableNameForAppid(appid) + " WHERE id = ?;");
			Row r = getClient().execute(ps.bind(key)).one();
			if (r != null) {
				logger.debug("Read id: " + key + " row: " + r);
				return fromRow(r.getString("json"), r.getString("json_updates"));
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
		return null;
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
			throwIfNecessary(e);
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
		try {
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
		} catch (Exception e) {
			logger.error(null, e);
			throwIfNecessary(e);
		}
		logger.debug("DAO.createAll() {}", objects.size());
	}

	@Override
	public <P extends ParaObject> Map<String, P> readAll(String appid, List<String> keys, boolean getAllColumns) {
		if (keys == null || keys.isEmpty() || StringUtils.isBlank(appid)) {
			return new LinkedHashMap<String, P>();
		}
		Map<String, P> results = new LinkedHashMap<String, P>(keys.size(), 0.75f, true);
		PreparedStatement ps = getPreparedStatement("SELECT id, json, json_updates FROM " +
				CassandraUtils.getTableNameForAppid(appid) + " WHERE id = ?;");

		List<CompletionStage<AsyncResultSet>> futures = new ArrayList<CompletionStage<AsyncResultSet>>(keys.size());
		for (String key : keys) {
			CompletionStage<AsyncResultSet> resultSetFuture = getClient().executeAsync(ps.bind(key));
			futures.add(resultSetFuture);
		}
		for (CompletionStage<AsyncResultSet> future : futures) {
			future.thenAccept(rows -> {
				Row row = rows.one();
				if (row != null) {
					String json = row.getString("json");
					String jsonUpdates = row.getString("json_updates");
					if (!StringUtils.isBlank(json)) {
						P obj = fromRow(json, jsonUpdates);
						results.put(row.getString("id"), obj);
					}
				}
			});
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
			Statement<?> st = SimpleStatement.newInstance("SELECT json, json_updates FROM " +
					CassandraUtils.getTableNameForAppid(appid) + ";");
			st.setPageSize(pager.getLimit());
			String lastPage = pager.getLastKey();
			if (lastPage != null) {
				if ("end".equals(lastPage)) {
					return results;
				} else {
					st.setPagingState(PagingState.fromString(lastPage));
				}
			}
			ResultSet rs = getClient().execute(st);
			PagingState nextPage = rs.getExecutionInfo().getSafePagingState();

			int remaining = rs.getAvailableWithoutFetching();
			for (Row row : rs) {
				if (row != null) {
					P obj = fromRow(row.getString("json"), row.getString("json_updates"));
					if (obj != null) {
						results.add(obj);
					}
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
			throwIfNecessary(e);
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
			throwIfNecessary(e);
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

	private <P extends ParaObject> P fromRow(String json, String jsonUpdates) {
		if (json == null || json.isEmpty()) {
			logger.debug("row is null or empty");
			return null;
		}
		try {
			P obj = ParaObjectUtils.fromJSON(json);
			if (obj != null) {
				if (jsonUpdates != null) {
					Map<String, Object> data =  ParaObjectUtils.getJsonReader(Map.class).readValue(jsonUpdates);
					ParaObjectUtils.setAnnotatedFields(obj, data, null);
				}
				return obj;
			}
		} catch (IOException ex) {
			logger.error(null, ex);
		}
		return null;
	}

	private static void throwIfNecessary(Throwable t) {
		if (t != null && Para.getConfig().exceptionOnWriteErrorsEnabled()) {
			throw new RuntimeException("DAO write operation failed!", t);
		}
	}

	//////////////////////////////////////////////////////

	@Override
	public <P extends ParaObject> String create(P so) {
		return create(Para.getConfig().getRootAppIdentifier(), so);
	}

	@Override
	public <P extends ParaObject> P read(String key) {
		return read(Para.getConfig().getRootAppIdentifier(), key);
	}

	@Override
	public <P extends ParaObject> void update(P so) {
		update(Para.getConfig().getRootAppIdentifier(), so);
	}

	@Override
	public <P extends ParaObject> void delete(P so) {
		delete(Para.getConfig().getRootAppIdentifier(), so);
	}

	@Override
	public <P extends ParaObject> void createAll(List<P> objects) {
		createAll(Para.getConfig().getRootAppIdentifier(), objects);
	}

	@Override
	public <P extends ParaObject> Map<String, P> readAll(List<String> keys, boolean getAllColumns) {
		return readAll(Para.getConfig().getRootAppIdentifier(), keys, getAllColumns);
	}

	@Override
	public <P extends ParaObject> List<P> readPage(Pager pager) {
		return readPage(Para.getConfig().getRootAppIdentifier(), pager);
	}

	@Override
	public <P extends ParaObject> void updateAll(List<P> objects) {
		updateAll(Para.getConfig().getRootAppIdentifier(), objects);
	}

	@Override
	public <P extends ParaObject> void deleteAll(List<P> objects) {
		deleteAll(Para.getConfig().getRootAppIdentifier(), objects);
	}

}
