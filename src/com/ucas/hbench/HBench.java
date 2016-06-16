package com.ucas.hbench;

public class HBench {
	
	private static String TEST_TABLE_NAME = "test";
	private static String TEST_COLUMN_FAMILY = "cf";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String testModule = args[0];
        int dataNum =Integer.parseInt(args[args.length-1]);
        HTest test = null;
        if(testModule.equals("hbase")){
        	test = new HbaseTest();
        }else if (testModule.equals("hydra")) {
			test = new HydraTest();
		}else if(testModule.equals("workload")){
			test = new GenDataSet();
		}else{
			System.err.println("请指定要测试的模块(hbase or hydra)");
			System.exit(0);
		}
        test.run(HBench.TEST_TABLE_NAME, HBench.TEST_COLUMN_FAMILY, dataNum);
	}

}
