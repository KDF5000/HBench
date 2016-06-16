package com.ucas.hbench;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

public class HbaseTest implements HTest {

	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param columnName
	 * @return
	 */
	public String get(String tableName, String rowKey, String family,String columnName) {
		// TODO Auto-generated method stub
		Table table = null;
		Connection connection = null;
		Configuration conf = null;
		try {
			conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(tableName));
			Get g = new Get(rowKey.getBytes());
			g.addColumn(family.getBytes(), columnName.getBytes());
			Result result = table.get(g);
			byte[] bytes = result.getValue(family.getBytes(), columnName.getBytes());
			String valueStr = new String(bytes);
			return valueStr;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(table!=null){
				try {
					table.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(connection !=null){
				try {
					connection.close();
				} catch (Exception e2) {
					// TODO: handle exception
					e2.printStackTrace();
				}
			}
		}
		return null;
	}
	/**
	 * 
	 * @param tableName
	 * @param columnNameFamily
	 * @param dataNum
	 */
	private void testHbase(String tableName, String columnNameFamily,int dataNum)  {
		long startTime = System.currentTimeMillis();// 获取当前时间
		Random r = new Random(555L);
		for (int i = 0; i < dataNum; i++) {
			int num = r.nextInt(dataNum);
			String tmp = num + "";
			this.get(tableName, tmp, columnNameFamily, tmp);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("读取时间：" + (endTime - startTime) + "ms");
	}


	@Override
	public void run(String tableName, String columnFamily, int dataNum) {
		this.testHbase(tableName, columnFamily, dataNum);
	}
}
