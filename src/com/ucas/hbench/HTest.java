package com.ucas.hbench;

public interface HTest {
	/**
	 * 运行测试
	 */
	public void run(String tableName, String columnFamily, int dataNum);
}
