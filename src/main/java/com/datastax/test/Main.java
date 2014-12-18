package com.datastax.test;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 */
public class Main {

	public Main(){
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String localDC = PropertyHelper.getProperty("localdc", "Cassandra");
		String remoteDC = PropertyHelper.getProperty("remotedc", "Analytics");
		
		Cluster cluster = Cluster.builder().addContactPoints(contactPointsStr.split(",")).build();
		Session session = cluster.connect();
		
		//Create keyspace and table 
		String createKeyspace = "create KEYSPACE if not exists datastax-testing WITH replication = {'class': 'NetworkTopologyStrategy' "
				+ "'"+ localDC + "': 1, '" + remoteDC + "': 1};";
		String createTable = "create table if not exists datastax-testing.latency (id text PRIMARY KEY)";

		String INSERT = "insert into datastax-testing.latency (id) values (?)";   
		
		PreparedStatement stmtLocal = session.prepare(INSERT);
		PreparedStatement stmtRemote = session.prepare(INSERT);
		
		stmtLocal.setConsistencyLevel(ConsistencyLevel.ONE);
		stmtRemote.setConsistencyLevel(ConsistencyLevel.QUORUM);
				
		session.execute(createKeyspace);
		session.execute(createTable);
		
		int counter = 0;
		
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
			long remoteWrite = local.getTimeTakenMillis();
			
			counter++;
			System.out.println("Local :" + localWrite + "ms Remote :" + remoteWrite + "ms Average local " + (localWrite/counter) 
					+ " Average Remote " + (remoteWrite/counter));
	
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
