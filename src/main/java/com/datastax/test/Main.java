package com.datastax.test;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

/**
 */
public class Main {

	public Main(){
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String localDC = PropertyHelper.getProperty("localdc", "Cassandra");
		String remoteDC = PropertyHelper.getProperty("remotedc", "Analytics");
		
		Cluster cluster = Cluster.builder().addContactPoints(contactPointsStr.split(","))
				.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(localDC))
				.build();
		
		Session session = cluster.connect();
		
		//Create keyspace and table 
		String createKeyspaceLocal = "create KEYSPACE if not exists datastax_testing_local WITH replication = {'class': 'NetworkTopologyStrategy', "
				+ "'"+ localDC + "': 1 };";
		String createKeyspaceRemote = "create KEYSPACE if not exists datastax_testing_remote WITH replication = {'class': 'NetworkTopologyStrategy', "
				+ "'"+ remoteDC + "': 1 };";
		String createTableLocal = "create table if not exists datastax_testing_local.latency (id text PRIMARY KEY)";
		String createTableRemote = "create table if not exists datastax_testing_remote.latency (id text PRIMARY KEY)";

		String INSERT_LOCAL = "insert into datastax_testing_local.latency (id) values (?)";
		String INSERT_REMOTE = "insert into datastax_testing_remote.latency (id) values (?)";
						
		session.execute(createKeyspaceLocal);
		session.execute(createTableLocal);
		session.execute(createKeyspaceRemote);
		session.execute(createTableRemote);
		
		PreparedStatement stmtLocal = session.prepare(INSERT_LOCAL);
		PreparedStatement stmtRemote = session.prepare(INSERT_REMOTE);
		
		stmtLocal.setConsistencyLevel(ConsistencyLevel.ONE);
		stmtRemote.setConsistencyLevel(ConsistencyLevel.ONE);

		int counter = 0;
		long localTotal = 0;
		long remoteTotal = 0;
		
		while (true){
			
			Timer local = new Timer();
			local.start();
			session.execute(stmtLocal.bind("" + System.currentTimeMillis()));
			local.end();
			long localWrite = local.getTimeTakenMillis();
			
			Timer remote = new Timer();
			remote.start();
			session.execute(stmtRemote.bind("" + System.currentTimeMillis()));
			remote.end();
			long remoteWrite = remote.getTimeTakenMillis();
			
			counter++;
			localTotal = localWrite + localTotal;
			remoteTotal = remoteWrite + remoteTotal;
			
			System.out.println("Local :" + localWrite + "ms Remote :" + remoteWrite + "ms Average local " + ((localTotal)/counter) 
					+ " Average Remote " + ((remoteTotal)/counter));
	
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		new Main();
	}

}
