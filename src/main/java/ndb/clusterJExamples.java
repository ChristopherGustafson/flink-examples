/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ndb;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.flink.contrib.streaming.state.schema.KeyValue;
import org.apache.flink.contrib.streaming.state.schema.KeyValueCommitted;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class clusterJExamples {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		//get the ClusterJ queryBuilder
		final SessionFactory dbSessionProvider = setupNDBSession();
		Session db = dbSessionProvider.getSession();

		readData(db);

		List<KeyValueCommitted> values =  readCommittedValueState(31,2, db);
		assert values.size() > 0;
		recoverActiveValueState(values, db);
	}

	private static SessionFactory setupNDBSession(){

		Properties props = new Properties();
		props.setProperty("com.mysql.clusterj.connectstring", "localhost");
		props.setProperty("com.mysql.clusterj.database",  "flinkndb");
		return ClusterJHelper.getSessionFactory(props);
	}

	private static void recoverActiveValueState(List<KeyValueCommitted> values, Session dbSession) {

		List<KeyValue> kvList = new ArrayList<>();

		for (KeyValueCommitted kvc : values) {
			KeyValue kv = dbSession.newInstance(KeyValue.class);

			kv.setKey(kvc.getKey());
			kv.setKeyGroup(kvc.getKeyGroup());
			kv.setStateName(kvc.getStateName());
			kv.setNameSpace(kvc.getNameSpace());
			kv.setEpoch(kvc.getEpoch());
			kv.setValue(kvc.getValue());

			kvList.add(kv);
		}

		if (kvList.size() > 0) {
			dbSession.savePersistentAll(kvList);
		}
	}

	private static List<KeyValueCommitted> readCommittedValueState(int kg, long epoch, Session dbSession) {

		//get the ClusterJ queryBuilder
		QueryBuilder qb = dbSession.getQueryBuilder();

		QueryDomainType<KeyValueCommitted> domainObject = qb.createQueryDefinition(KeyValueCommitted.class);

		Predicate kgPredicate = domainObject.get("keyGroup").equal(domainObject.param("keyGroup"));
		Predicate nsPredicate = domainObject.get("nameSpace").equal(domainObject.param("namespace"));
		Predicate epochPredicate = domainObject.get("epoch").equal(domainObject.param("epoch"));
		domainObject.where(kgPredicate.and(nsPredicate).and(epochPredicate));

		Query<KeyValueCommitted> query = dbSession.createQuery(domainObject);
		query.setParameter("keyGroup", kg);
		query.setParameter("namespace", 99); //TODO: hardcode
		query.setParameter("epoch", epoch);

		List<KeyValueCommitted> results = null;
		try {
			results = query.getResultList();
			if (results.size() > 0) {

			}

		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while reading committed value state for recovery.", e);
		}

		return results;
	}

	private static void readData(Session dbSession)
	{
		QueryBuilder qb = dbSession.getQueryBuilder();

		QueryDomainType<KeyValue> domainObject = qb.createQueryDefinition(KeyValue.class);

		Predicate kg = domainObject.get("keyGroup").equal(domainObject.param("keyGroup"));
		Predicate ep = domainObject.get("epoch").equal(domainObject.param("epoch"));
		Predicate ns = domainObject.get("nameSpace").equal(domainObject.param("namespace"));


		domainObject.where(kg.and(ns).and(ep)); //.and(ep).and(ns));
		long epoch = 2;
		Query<KeyValue> query = dbSession.createQuery(domainObject);
		query.setParameter("keyGroup", 31);
		query.setParameter("namespace", 99);
		query.setParameter("epoch", epoch);


		int results = -1;
		try {
			results = query.deletePersistentAll();
			if(results > 0){

			}
		} finally {

		}
	}

}
