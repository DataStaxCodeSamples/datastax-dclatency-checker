package com.datastax.test;

import java.io.UnsupportedEncodingException;

import com.datastax.demo.utils.FileUtils;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.google.common.io.BaseEncoding;

/**
 */
public class Main {

	public Main() {
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String FILE_NAME = PropertyHelper.getProperty("file", "smallfile1K");
		String localDC = PropertyHelper.getProperty("localdc", "Cassandra");
		String remoteDC = PropertyHelper.getProperty("remotedc", "Analytics");

		Cluster cluster = Cluster.builder().addContactPoints(contactPointsStr.split(","))
				.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(localDC)).build();

		Session session = cluster.connect();

		// Create keyspace and table
		String createKeyspaceLocal = "create KEYSPACE if not exists datastax_testing_local WITH replication = {'class': 'NetworkTopologyStrategy', "
				+ "'" + localDC + "': 1 };";
		String createKeyspaceRemote = "create KEYSPACE if not exists datastax_testing_remote WITH replication = {'class': 'NetworkTopologyStrategy', "
				+ "'" + remoteDC + "': 1 };";
		String createTableLocal = "create table if not exists datastax_testing_local.latency (id text PRIMARY KEY, value text) WITH default_time_to_live = 25;";
		String createTableRemote = "create table if not exists datastax_testing_remote.latency (id text PRIMARY KEY, value text) WITH default_time_to_live = 25;";

		String INSERT_LOCAL = "insert into datastax_testing_local.latency (id, value) values (?,?)";
		String INSERT_REMOTE = "insert into datastax_testing_remote.latency (id, value) values (?,?)";

		session.execute(createKeyspaceLocal);
		session.execute(createTableLocal);
		session.execute(createKeyspaceRemote);
		session.execute(createTableRemote);

		PreparedStatement stmtLocal = session.prepare(INSERT_LOCAL);
		PreparedStatement stmtRemote = session.prepare(INSERT_REMOTE);

		stmtLocal.setConsistencyLevel(ConsistencyLevel.ONE);
		stmtRemote.setConsistencyLevel(ConsistencyLevel.ONE);

		MovingAverage localMA = new MovingAverage(20);
		MovingAverage remoteMA = new MovingAverage(20);
		
		
		String inputContent = FileUtils.readFileIntoString(FILE_NAME);
		String base64String = "";
		try {
			base64String = BaseEncoding.base64().encode(inputContent.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
			System.exit(1);
		}

		while (true) {
			Timer local = new Timer();
			Timer remote = new Timer();

			try {
				local.start();
				session.execute(stmtLocal.bind("id", base64String));
				local.end();

				remote.start();
				session.execute(stmtRemote.bind("id", base64String));
				remote.end();
				
			} catch (WriteTimeoutException e) {
				System.out.println("Caught exception - " + e.getMessage());
				continue;
			}

			long localWrite = local.getTimeTakenMillis();
			long remoteWrite = remote.getTimeTakenMillis();

			localMA.newNum(localWrite);
			remoteMA.newNum(remoteWrite);

			System.out.println("Local :" + localWrite + "ms Remote :" + remoteWrite + "ms Average local "
					+ localMA.getAvg() + " Average Remote " + remoteMA.getAvg());

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		new Main();
	}

}
