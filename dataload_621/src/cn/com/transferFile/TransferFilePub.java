package cn.com.transferFile;

import java.util.HashMap;

//import cn.com.pub.PubAPIs;

public class TransferFilePub {

	private static HashMap<String, Integer> threadhashMap = new HashMap<String, Integer>();

	public static synchronized int threadLock(String thread_id, String type) {
		//PubAPIs pubAPIs = new PubAPIs("thread_num_test");
		Integer tmp = threadhashMap.get(thread_id);
		int thread_num = tmp != null ? tmp : 0;
		if ("add".equals(type)) {
			thread_num++;
			threadhashMap.put(thread_id, thread_num);
			//pubAPIs.writerLog("thread_num+:"+thread_num);
		} else if ("minus".equals(type)) {
			thread_num--;
			//pubAPIs.writerLog("thread_num-:"+thread_num);
			threadhashMap.put(thread_id, thread_num);
		} else if ("del".equals(type)) {
			threadhashMap.remove(thread_id);
		}
		return thread_num;
	}
	private static HashMap<String, Integer> PushThreadHashMap1 = new HashMap<String, Integer>();
	private static HashMap<String, Integer> PushThreadHashMap2 = new HashMap<String, Integer>();

	public static synchronized int[] threadCheckPushThread(String tabName, String type) {
		Integer tmp1 = PushThreadHashMap1.get(tabName);
		int thread_num1 = tmp1 != null ? PushThreadHashMap1.get(tabName) : 0;
		Integer tmp2 = PushThreadHashMap2.get(tabName);
		int thread_num2 = tmp2 != null ? PushThreadHashMap2.get(tabName) : 0;
		if ("add".equals(type)) {
			thread_num1++;
		} else if ("minus".equals(type)) {
			thread_num1--;
		} else if ("add2".equals(type)) {
			thread_num2++;
		}
		PushThreadHashMap1.put(tabName, thread_num1);
		PushThreadHashMap2.put(tabName, thread_num2);
		int[] result = { thread_num1, thread_num2 };
		return result;
	}
}
