package com.ucas.hbench;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.hydra.hydracache.client.HydraCacheClientImpl;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

public class HydraTest implements HTest {
	private String tableName;
	private String columnFamily;
	
	@Override
	public void run(String tableName, String columnFamily, int dataNum) {
		// TODO Auto-generated method stub
		this.tableName = tableName;
		this.columnFamily = columnFamily;
		
		List<HostAndPort> list = new ArrayList<HostAndPort>();
		list.add(new HostAndPort("127.0.0.1", 7000));
		list.add(new HostAndPort("127.0.0.1", 7001));
		// test no cache
//		System.out.println("test no cache:");
//		testHydracachePart(dataNum, false, null, false);
		
		// test redies cache
		System.out.println("test redis cache:");
		this.cleanAll();
		testHydracachePart(dataNum, true, list, false);
		
		// test local cache
		System.out.println("test only loacl cache:");
		testHydracachePart(dataNum, false, list, true);
		
		// test redies cache and local Cache
		System.out.println("test redis cache and local Cache:");
		this.cleanAll();
		testHydracachePart(dataNum, true, list, true);
	}
	/**
	 * 
	 */
	private void cleanAll(){
		for(int i=0;i<3;i++){
			Jedis node = new Jedis("127.0.0.1", 7000+i);
			node.flushAll();
			node.close();
		}
	}
	public  void testHydracachePart(int dataNum, boolean cacheOn,
			List<HostAndPort> list, boolean localCacheOn) {
		HydraCacheClientImpl client = new HydraCacheClientImpl(cacheOn, list,
				localCacheOn);
		long startTime = System.currentTimeMillis();
		Random r = new Random(555L);
		for (int i = 0; i < dataNum; i++) {
			int num = r.nextInt(dataNum);
			String tem = num + "";
//			System.out.println("get key:"+tem);
			String content = client.get(this.tableName, tem, this.columnFamily, tem, 20);
			// String valString = client.get("user", "1", "cf", "1", 20);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("读取时间：" + (endTime - startTime) + "ms");
	}

}
