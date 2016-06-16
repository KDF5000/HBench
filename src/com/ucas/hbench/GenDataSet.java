package com.ucas.hbench;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GenDataSet implements HTest {
	/**
	 * 
	 * @param tableName
	 * @param columnNameFamily
	 * @param dataNum
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	private void createTable(String tableName, String columnNameFamily,	int dataNum){
		Logger.getRootLogger().setLevel(Level.WARN);
		HTableDescriptor htd = new HTableDescriptor(
				TableName.valueOf(tableName));
		// create column descriptor
		htd.addFamily(new HColumnDescriptor(columnNameFamily));
		// configure HBase
		Configuration configuration = HBaseConfiguration.create();
		HBaseAdmin hAdmin;
		try {
			hAdmin = new HBaseAdmin(configuration);
			if (hAdmin.tableExists(tableName)) {
				try {
					hAdmin.disableTable(tableName);
					hAdmin.deleteTable(tableName);
					System.out.println("Table already exists,delete....");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			hAdmin.createTable(htd);
			System.out.println("table '" + tableName + "' created successfully");
			hAdmin.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	/**
	 * 
	 * @param usertableName
	 * @param columnNameFamily
	 * @param dataNum
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	private void putData(String usertableName,String columnNameFamily, int dataNum){
		Configuration configuration = HBaseConfiguration.create();
		HTable table = null;
		try {
			table = new HTable(configuration, usertableName);
			for (int i = 0; i < dataNum; i++) {
				String tmp = i + "";
				Put put = new Put(tmp.getBytes());
				put.add(columnNameFamily.getBytes(), tmp.getBytes(), tmp.getBytes());
				table.put(put);
			}
			System.out.println("generate "+dataNum+"　data!");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}finally{
			try {
				if(table!=null){
					table.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	@Override
	public void run(String tableName, String columnFamily, int dataNum) {
		// TODO Auto-generated method stub
		this.createTable(tableName, columnFamily, dataNum);
		this.putData(tableName, columnFamily, dataNum);
	}

}
