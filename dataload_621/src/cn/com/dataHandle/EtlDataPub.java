package cn.com.dataHandle;

import java.io.File;


import java.io.FileOutputStream;

import java.io.PrintStream;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
//import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import java.util.Properties;


//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
//import org.apache.hadoop.security.UserGroupInformation;

import cn.com.dataHandle.bean.TranfersDataBean;
import cn.com.pub.PubAPIs;
public class EtlDataPub {
	

		private static PubAPIs pubAPIs = new PubAPIs("dataHandle");
		private static HashMap<String, Integer> threadhashMap = new HashMap<String, Integer>();
		public static synchronized boolean checkSQLFile(String  end_file_path) throws Exception {
			return new File(end_file_path).exists();
		}
		public static synchronized int threadLock(String lockObject, String type, String sou_tab, int lock_table_num) {
			Integer tmp = threadhashMap.get(lockObject);
			int thread_num = tmp != null ? tmp : 0;
			if ("add".equals(type)) {
				thread_num++;
				threadhashMap.put(lockObject, thread_num);
				// threadLockPubAPIs.writerLog(sou_tab + "->" + lockObject +
				// "|add");
			} else if ("minus".equals(type)) {
				thread_num--;
				threadhashMap.put(lockObject, thread_num);
				// threadLockPubAPIs.writerLog(sou_tab + "->" + lockObject +
				// "|minus");
			} else if ("del".equals(type)) {
				threadhashMap.remove(lockObject);
			} else if ("check".equals(type)) {
				if (thread_num < lock_table_num) {// -1表达得到了跑权限
					thread_num++;
					threadhashMap.put(lockObject, thread_num);
					// threadLockPubAPIs.writerLog(sou_tab + "->" + lockObject +
					// "|check");
					return -1;
				}
			}
			return thread_num;
		}

		private static HashMap<String, Integer> DataExportHashMap1 = new HashMap<String, Integer>();
		private static HashMap<String, Integer> DataExportHashMap2 = new HashMap<String, Integer>();

		public static synchronized int[] threadCheckDataExport(String tabName, String type) {
			Integer tmp1 = DataExportHashMap1.get(tabName);
			int thread_num1 = tmp1 != null ? DataExportHashMap1.get(tabName) : 0;
			Integer tmp2 = DataExportHashMap2.get(tabName);
			int thread_num2 = tmp2 != null ? DataExportHashMap2.get(tabName) : 0;
			if ("add".equals(type)) {
				thread_num1++;
			} else if ("minus".equals(type)) {
				thread_num1--;
			} else if ("add2".equals(type)) {
				thread_num2++;
			}
			DataExportHashMap1.put(tabName, thread_num1);
			DataExportHashMap2.put(tabName, thread_num2);
			int[] result = { thread_num1, thread_num2 };
			return result;
		}

		private static HashMap<String, Integer> DataImportHashMap1 = new HashMap<String, Integer>();
		private static HashMap<String, Integer> DataImportHashMap2 = new HashMap<String, Integer>();

		public static synchronized int[] threadCheckDataImport(String tabName, String type) {
			Integer tmp1 = DataImportHashMap1.get(tabName);
			int thread_num1 = tmp1 != null ? DataImportHashMap1.get(tabName) : 0;
			Integer tmp2 = DataImportHashMap2.get(tabName);
			int thread_num2 = tmp2 != null ? DataImportHashMap2.get(tabName) : 0;
			if ("add".equals(type)) {
				thread_num1++;
			} else if ("minus".equals(type)) {
				thread_num1--;
			} else if ("add2".equals(type)) {
				thread_num2++;
			}
			DataImportHashMap1.put(tabName, thread_num1);
			DataImportHashMap2.put(tabName, thread_num2);
			int[] result = { thread_num1, thread_num2 };
			return result;
		}

		public static synchronized Connection createTDHConn(String authUser) {
			Connection conn = null;
			try {
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				String authType = pubAPIs.getProperty2(authUser, "authType", "`", ";");
				String db_inceptorAddr = pubAPIs.getProperty2(authUser, "db_inceptorAddr", "`", ";");
				String db_user_id = pubAPIs.getProperty2(authUser, "db_user_id", "`", ";");
				String db_user_pwd = pubAPIs.getProperty2(authUser, "db_user_pwd", "`", ";");
				String db_keytab = pubAPIs.getProperty2(authUser, "db_keytab", "`", ";");
				String db_principal = pubAPIs.getProperty2(authUser, "db_principal", "`", ";");
				String db_kuser = pubAPIs.getProperty2(authUser, "db_kuser", "`", ";");
				String db_krb5conf = pubAPIs.getProperty2(authUser, "db_krb5conf", "`", ";");

				String urlStr = "jdbc:hive2://" + db_inceptorAddr + "/default";
				if ("none".equals(authType)) {
					conn = DriverManager.getConnection(urlStr);
				} else if ("ldap".equals(authType)) {
					urlStr = "jdbc:hive2://" + db_inceptorAddr + "/default";
					conn = DriverManager.getConnection(urlStr, db_user_id, db_user_pwd);
				} else if ("kerberos".equals(authType)) {
					urlStr = "jdbc:hive2://" + db_inceptorAddr + "/default;principal=" + db_principal
							+ ";authentication=kerberos;kuser=" + db_kuser + ";keytab=" + db_keytab + ";krb5conf="
							+ db_krb5conf + ";";
					conn = DriverManager.getConnection(urlStr);
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
				return conn;
			}
			return conn;
		}

		public static synchronized Connection createTDHConnSM4(String authUser) {
			Connection conn = null;
			try {
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				String authType = pubAPIs.getProperty2(authUser, "authType", "`", ";");
				String db_inceptorAddr = pubAPIs.getProperty2(authUser, "db_inceptorAddr", "`", ";");
				String db_user_id = pubAPIs.getProperty2(authUser, "db_user_id", "`", ";");
				String db_user_pwd = pubAPIs.getProperty2(authUser, "db_user_pwd", "`", ";");
				String db_keytab = pubAPIs.getProperty2(authUser, "db_keytab", "`", ";");
				String db_principal = pubAPIs.getProperty2(authUser, "db_principal", "`", ";");
				String db_kuser = pubAPIs.getProperty2(authUser, "db_kuser", "`", ";");
				String db_krb5conf = pubAPIs.getProperty2(authUser, "db_krb5conf", "`", ";");
				String urlStr = "jdbc:hive2://" + db_inceptorAddr + "/default";
				if ("none".equals(authType)) {
					conn = DriverManager.getConnection(urlStr);
				} else if ("ldap".equals(authType)) {
					urlStr = "jdbc:hive2://" + db_inceptorAddr + "/default";
					db_user_pwd = pubAPIs.getPassText(db_user_pwd);
					conn = DriverManager.getConnection(urlStr, db_user_id, db_user_pwd);
				} else if ("kerberos".equals(authType)) {
					urlStr = "jdbc:hive2://" + db_inceptorAddr + "/default;principal=" + db_principal
							+ ";authentication=kerberos;kuser=" + db_kuser + ";keytab=" + db_keytab + ";krb5conf="
							+ db_krb5conf + ";";
					conn = DriverManager.getConnection(urlStr);
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
				return conn;
			}
			return conn;
		}

		public static synchronized Connection createMysqlConnSM4(String authUser) {
			Connection conn = null;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				String db_mysqlAddr = pubAPIs.getProperty2(authUser, "db_mysqlAddr", "`", ";");
				String db_user_id = pubAPIs.getProperty2(authUser, "db_user_id", "`", ";");
				String db_user_pwd = pubAPIs.getProperty2(authUser, "db_user_pwd", "`", ";");
				db_user_pwd = pubAPIs.getPassText(db_user_pwd);
				String urlStr = "jdbc:mysql://" + db_mysqlAddr + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
				conn = DriverManager.getConnection(urlStr, db_user_id, db_user_pwd);
				//System.out.println(conn);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return conn;
			}
			return conn;
		}

		public static synchronized Connection createMysqlConn(String authUser) {
			Connection conn = null;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				String db_mysqlAddr = pubAPIs.getProperty2(authUser, "db_mysqlAddr", "`", ";");
				String db_user_id = pubAPIs.getProperty2(authUser, "db_user_id", "`", ";");
				String db_user_pwd = pubAPIs.getProperty2(authUser, "db_user_pwd", "`", ";");
				// db_user_pwd = pubAPIs.getPassText(db_user_pwd);
				String urlStr = "jdbc:mysql://" + db_mysqlAddr + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
				conn = DriverManager.getConnection(urlStr, db_user_id, db_user_pwd);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return conn;
			}
			return conn;
		}
		
		public static synchronized Connection createOracleConnSM4(String authUser) {
			Connection conn = null;
			try {
				
				Class.forName("oracle.jdbc.OracleDriver");
				String db_oracleAddr = pubAPIs.getProperty2(authUser, "db_oracleAddr", "`", ";");
				String db_user_id = pubAPIs.getProperty2(authUser, "db_user_id", "`", ";");
				String db_user_pwd = pubAPIs.getProperty2(authUser, "db_user_pwd", "`", ";");
				db_user_pwd = pubAPIs.getPassText(db_user_pwd);
				String urlStr = "jdbc:oracle:thin:@"+db_oracleAddr;
				conn = DriverManager.getConnection(urlStr, db_user_id, db_user_pwd);
				//conn = DriverManager.getConnection("jdbc:oracle:thin:@172.20.235.177:1521:orcl","tdh","tdh");
				//System.out.println(conn);

			} catch (Exception e) {

				e.printStackTrace();
				return conn;
			}
			return conn;
		}
		public String lincoon(String authUser) {
		
			String db_user_id = pubAPIs.getProperty2(authUser, "db_user_id", "`", ";");
			String db_user_pwd = pubAPIs.getProperty2(authUser, "db_user_pwd", "`", ";");
			db_user_pwd = pubAPIs.getPassText(db_user_pwd);
			String lincn=db_user_id+"/"+db_user_pwd+"@"+db_user_id;
			//System.out.println(lincn);
			return lincn;
			
		}
		/*private static Configuration conf = null;
		public static synchronized FileSystem getFileSystem(String conf_hdfs, String login_kerberos) throws Exception {
			String login_type = pubAPIs.getProperty2(login_kerberos, "type", "`", ";");
			String login_krb5conf = pubAPIs.getProperty2(login_kerberos, "krb5conf", "`", ";");
			String login_username = pubAPIs.getProperty2(login_kerberos, "username", "`", ";");
			String login_password = pubAPIs.getProperty2(login_kerberos, "password", "`", ";");

			String l_nameNodeIPnn1 = "";
			String l_nameNodeIPnn2 = "";
			String l_nameNodeIP = pubAPIs.getProperty2(conf_hdfs, "nameNodeIP", "`", ";");
			if (!"".equals(l_nameNodeIP) && l_nameNodeIP.indexOf(",") != -1) {
				l_nameNodeIPnn1 = l_nameNodeIP != null ? l_nameNodeIP.split(",")[0] : "";
				l_nameNodeIPnn2 = l_nameNodeIP != null ? l_nameNodeIP.split(",")[1] : "";
			}
			conf = pubAPIs.getBaseConf();
			conf.set("dfs.namenode.rpc-address.nameservice1.nn1", l_nameNodeIPnn1);
			conf.set("dfs.namenode.rpc-address.nameservice1.nn2", l_nameNodeIPnn2);

			FileSystem fs = null;
			if ("keytab".equals(login_type)) {
				System.setProperty("java.security.krb5.conf", login_krb5conf);
				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation app_ugi = UserGroupInformation.loginUserFromPasswordAndReturnUGI(login_username,
						login_password);
				fs = app_ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
					public FileSystem run() throws Exception {
						// FileSystem fs = FileSystem.newInstance(conf);
						FileSystem fs = FileSystem.get(conf);
						fs.getStatus();
						return fs;
					}
				});
			} else {
				System.setProperty("HADOOP_USER_NAME", login_username);
				fs = FileSystem.get(conf);
				fs.getStatus();
			}
			return fs;
		}*/

		public int getProcessID() {
			RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
			return Integer.valueOf(runtimeMXBean.getName().split("@")[0]).intValue();
		}

		// 判断对应目录是否存在新数据
		public static synchronized int checkHisData(FileSystem fs, String dataPath, String etlDate) throws Exception {
			int result = 0;// -1数据有变地;-2不存在表示被DROP了/被truncate了;0没有变动
			//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			int etl_date = Integer.parseInt(etlDate);
			Path filePath = new Path(dataPath);
			if (!fs.exists(filePath)) {// 不存在,表示被DROP了
				result = -3;
				return result;
			}
			long fileSize = 0;
			
			RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(filePath, true);
			// 递归列出该目录下所有文件，不包括文件夹，后面的布尔值为是否递归
			int mun = 0;// 等于0表示被truncate了
			while (listFiles.hasNext()) {// 如果listfiles里还有东西
				mun++;
				LocatedFileStatus next = listFiles.next();// 得到下一个并pop出listFiles
				fileSize =fileSize + fs.getContentSummary(next.getPath()).getLength();
				long time = next.getModificationTime();
				Date fileDate = new Date(time);
				int fDate = Integer.parseInt(sdf.format(fileDate));
				if (fDate >= etl_date) {
					result = -1;// 数据有变动
					return result;
				}
			}
			if (mun == 0) {
				result = -2;
			}
			if(fileSize==0){
				result = -2;
				return result;
			}
			return result;
		}
		public static String getDirSign() {
			String dirSign = "";
			Properties props = System.getProperties(); // 获得系统属性集
			String osName1 = props.getProperty("os.name"); // 操作系统名称
			if (osName1.indexOf("Windows") != -1) {
				dirSign = "\\";
			} else {
				dirSign = "/";
			}
			return dirSign;
		}
		public static synchronized void addStringToFile(String file_path, String info,String lastChar) throws Exception {
			if ("".equals(info.trim().replace("\n", ""))) {
				return;
			}
			File file = new File(file_path);
			if (!file.exists()) {
				file.createNewFile();
			}
			PrintStream ps = new PrintStream(new FileOutputStream(file, true));
			if (!"".equals(info)) {
				String info_lastChar = info.substring(info.length() - 1, info.length());
				if ("\n".equals(info_lastChar)) {
					info = info.substring(0, info.length() - 1);
				}
			}
			info += lastChar;
			ps.append(info);
			//pubAPIs.writerLog(file_path + ":\n" + info);
			ps.close();
		}
		public void delete(File file) {
			if (!file.exists())
				return;

			if (file.isFile() || file.list() == null) {
				file.delete();
			} else {
				File[] files = file.listFiles();
				for (File a : files) {
					delete(a);
				}
				file.delete();
			}
		}
		
		public static synchronized void write_exp_jobLog(TranfersDataBean tranfersDataBean, String type, String job_info) {
			PreparedStatement stmt = null;
			Connection conn_mysql = null;
			long endTime = new Date().getTime();
			int tatol = (int) ((endTime - tranfersDataBean.getStartTime()) * 1 / 1000);
			String id = "";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			if (job_info.length() > 5000) {
				pubAPIs.writerLog("job_info too log:" + job_info);
				job_info = job_info.substring(0, 5000);
			}
			try {
				conn_mysql = createMysqlConnSM4(tranfersDataBean.getMysql_login_ldap());
				if ("0".equals(type)) {// 开始
					/*String sql = "delete from transfer_exp_job_log where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, id);
					stmt.addBatch();
					stmt.executeBatch();
					stmt.close();*/
					String start_time =sdf.format(new Date());
					id=tranfersDataBean.getJob_id() + "\001" + start_time.replace("-", "").replace(":", "").replace(" ", "\001");
					tranfersDataBean.setLog_id(id);
					String sql = "insert into transfer_exp_job_log(id,batch_id,job_id,task_status,exec_type,database_name,table_name,etl_date,"
							+ "src_count,start_time,tar_count,job_info,table_full_name,tactics_id) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, id);
					stmt.setString(2, tranfersDataBean.getBatch_id());
					stmt.setString(3, tranfersDataBean.getJob_id());
					stmt.setString(4, "running");
					stmt.setString(5, tranfersDataBean.getExec_type());
					stmt.setString(6, tranfersDataBean.getDb_name());
					stmt.setString(7, tranfersDataBean.getTab_name());
					stmt.setString(8, tranfersDataBean.getEtl_date());
					stmt.setInt(9, tranfersDataBean.getJob_src_count());
					stmt.setString(10, sdf.format(new Date()));
					stmt.setInt(11, tranfersDataBean.getJob_tar_count());
					stmt.setString(12, job_info);
					stmt.setString(13, tranfersDataBean.getTab_full_name());
					stmt.setString(14, tranfersDataBean.getTactics_id());
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("1".equals(type)) {//
					id=tranfersDataBean.getLog_id();
					String sql = "update transfer_exp_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, sdf.format(new Date()));
					if(tranfersDataBean.getType()==3) {
						stmt.setString(3, "error1");
					}else {
						stmt.setString(3, "error");
					}
					stmt.setInt(4, tatol);
					stmt.setInt(5, tranfersDataBean.getJob_src_count());
					stmt.setInt(6, tranfersDataBean.getJob_tar_count());
					stmt.setString(7, id);
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("2".equals(type)) {
					id=tranfersDataBean.getLog_id();
					String sql = "update transfer_exp_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, sdf.format(new Date()));
					stmt.setString(3, "success");
					stmt.setInt(4, tatol);
					stmt.setInt(5, tranfersDataBean.getJob_src_count());
					stmt.setInt(6, tranfersDataBean.getJob_tar_count());
					stmt.setString(7, id);
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("3".equals(type)) {
					id=tranfersDataBean.getLog_id();
					String sql = "update transfer_exp_job_log set job_info=?,task_status=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, "running");
					stmt.setString(3, id);
					stmt.addBatch();
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
			} finally {
				try {
					stmt.close();
					conn_mysql.close();
				} catch (SQLException e) {
					String eStr = pubAPIs.getException(e);
					pubAPIs.writerLog(eStr);
				}
			}
		}
		public static synchronized void write_imp_jobLog(TranfersDataBean tranfersDataBean, String type, String job_info) {
			PreparedStatement stmt = null;
			Connection conn_mysql = null;
			long endTime = new Date().getTime();
			int tatol = (int) ((endTime - tranfersDataBean.getStartTime()) * 1 / 1000);
			String id = "";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			if (job_info.length() > 5000) {
				pubAPIs.writerLog("job_info too log:" + job_info);
				job_info = job_info.substring(0, 5000);
			}
			try {
				conn_mysql = createMysqlConnSM4(tranfersDataBean.getMysql_login_ldap());
				if ("0".equals(type)) {// 开始
	/*				String sql = "delete from transfer_imp_job_log where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, id);
					stmt.addBatch();
					stmt.executeBatch();
					stmt.close();*/
					String start_time =sdf.format(new Date());
					id=tranfersDataBean.getJob_id() + "\001" + start_time.replace("-", "").replace(":", "").replace(" ", "\001");
					tranfersDataBean.setLog_id(id);
					String	sql = "insert into transfer_imp_job_log(id,batch_id,job_id,task_status,exec_type,database_name,table_name,etl_date,"
							+ "src_count,start_time,tar_count,job_info,table_full_name,tactics_id) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, id);
					stmt.setString(2, tranfersDataBean.getBatch_id());
					stmt.setString(3, tranfersDataBean.getJob_id());
					stmt.setString(4, "running");
					stmt.setString(5, tranfersDataBean.getExec_type());
					stmt.setString(6, tranfersDataBean.getDb_name());
					stmt.setString(7, tranfersDataBean.getTab_name());
					stmt.setString(8, tranfersDataBean.getEtl_date());
					stmt.setInt(9, tranfersDataBean.getJob_src_count());
					stmt.setString(10, sdf.format(new Date()));
					stmt.setInt(11, tranfersDataBean.getJob_tar_count());
					stmt.setString(12, job_info);
					stmt.setString(13, tranfersDataBean.getTab_full_name());
					stmt.setString(14, tranfersDataBean.getTactics_id());
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("1".equals(type)) {//
					id=tranfersDataBean.getLog_id();
					String sql = "update transfer_imp_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, sdf.format(new Date()));
					if(tranfersDataBean.getType()==3) {
						stmt.setString(3, "error1");
					}else {
						stmt.setString(3, "error");
					}
					stmt.setInt(4, tatol);
					stmt.setInt(5, tranfersDataBean.getJob_src_count());
					stmt.setInt(6, tranfersDataBean.getJob_tar_count());
					stmt.setString(7, id);
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("2".equals(type)) {
					id=tranfersDataBean.getLog_id();
					String sql = "update transfer_imp_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, sdf.format(new Date()));
					stmt.setString(3, "success");
					stmt.setInt(4, tatol);
					stmt.setInt(5, tranfersDataBean.getJob_src_count());
					stmt.setInt(6, tranfersDataBean.getJob_tar_count());
					stmt.setString(7, id);
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("3".equals(type)) {
					id=tranfersDataBean.getLog_id();
					String sql = "update transfer_imp_job_log set job_info=?,task_status=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, "running");
					stmt.setString(3, id);
					stmt.addBatch();
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
			} finally {
				try {
					stmt.close();
					conn_mysql.close();
				} catch (SQLException e) {
					String eStr = pubAPIs.getException(e);
					pubAPIs.writerLog(eStr);
				}
			}
		}
		
		
		public static synchronized void write_etlexp_jobLog(TranfersDataBean tranfersDataBean, String type, String job_info) {
			PreparedStatement stmt = null;
			Connection conn_mysql = null;
			long endTime = new Date().getTime();
			int tatol = (int) ((endTime - tranfersDataBean.getStartTime()) * 1 / 1000);
			String id = "";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			if (job_info.length() > 5000) {
				pubAPIs.writerLog("job_info too log:" + job_info);
				job_info = job_info.substring(0, 5000);
			}
			try {
				conn_mysql = createMysqlConnSM4(tranfersDataBean.getMysql_login_ldap());
				if ("0".equals(type)) {// 开始
					/*String sql = "delete from transfer_exp_job_log where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, id);
					stmt.addBatch();
					stmt.executeBatch();
					stmt.close();*/
					String start_time =sdf.format(new Date());
					id=tranfersDataBean.getJob_id() + "\001" + start_time.replace("-", "").replace(":", "").replace(" ", "\001");
					tranfersDataBean.setLog_id(id);
					String sql = "insert into etl_exp_job_log(id,batch_id,job_id,task_status,exec_type,database_name,table_name,etl_date,"
							+ "src_count,start_time,tar_count,job_info,table_full_name,tactics_id) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, id);
					stmt.setString(2, tranfersDataBean.getBatch_id());
					stmt.setString(3, tranfersDataBean.getJob_id());
					stmt.setString(4, "running");
					stmt.setString(5, tranfersDataBean.getExec_type());
					stmt.setString(6, tranfersDataBean.getDb_name());
					stmt.setString(7, tranfersDataBean.getTab_name());
					stmt.setString(8, tranfersDataBean.getEtl_date());
					stmt.setInt(9, tranfersDataBean.getJob_src_count());
					stmt.setString(10, sdf.format(new Date()));
					stmt.setInt(11, tranfersDataBean.getJob_tar_count());
					stmt.setString(12, job_info);
					stmt.setString(13, tranfersDataBean.getTab_full_name());
					stmt.setString(14, tranfersDataBean.getTactics_id());
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("1".equals(type)) {//
					id=tranfersDataBean.getLog_id();
					String sql = "update etl_exp_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, sdf.format(new Date()));
					if(tranfersDataBean.getType()==3) {
						stmt.setString(3, "error1");
					}else {
						stmt.setString(3, "error");
					}
					stmt.setInt(4, tatol);
					stmt.setInt(5, tranfersDataBean.getJob_src_count());
					stmt.setInt(6, tranfersDataBean.getJob_tar_count());
					stmt.setString(7, id);
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("2".equals(type)) {
					id=tranfersDataBean.getLog_id();
					String sql = "update etl_exp_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, sdf.format(new Date()));
					stmt.setString(3, "success");
					stmt.setInt(4, tatol);
					stmt.setInt(5, tranfersDataBean.getJob_src_count());
					stmt.setInt(6, tranfersDataBean.getJob_tar_count());
					stmt.setString(7, id);
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("3".equals(type)) {
					id=tranfersDataBean.getLog_id();
					String sql = "update etl_exp_job_log set job_info=?,task_status=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, "running");
					stmt.setString(3, id);
					stmt.addBatch();
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
			} finally {
				try {
					stmt.close();
					conn_mysql.close();
				} catch (SQLException e) {
					String eStr = pubAPIs.getException(e);
					pubAPIs.writerLog(eStr);
				}
			}
		}
		public static synchronized void write_etlimp_jobLog(TranfersDataBean tranfersDataBean, String type, String job_info) {
			PreparedStatement stmt = null;
			Connection conn_mysql = null;
			long endTime = new Date().getTime();
			int tatol = (int) ((endTime - tranfersDataBean.getStartTime()) * 1 / 1000);
			String id = "";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			if (job_info.length() > 5000) {
				pubAPIs.writerLog("job_info too log:" + job_info);
				job_info = job_info.substring(0, 5000);
			}
			try {
				conn_mysql = createMysqlConnSM4(tranfersDataBean.getMysql_login_ldap());
				if ("0".equals(type)) {// 开始
	/*				String sql = "delete from transfer_imp_job_log where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, id);
					stmt.addBatch();
					stmt.executeBatch();
					stmt.close();*/
					String start_time =sdf.format(new Date());
					id=tranfersDataBean.getJob_id() + "\001" + start_time.replace("-", "").replace(":", "").replace(" ", "\001");
					tranfersDataBean.setLog_id(id);
					String	sql = "insert into etl_imp_job_log(id,batch_id,job_id,task_status,exec_type,database_name,table_name,etl_date,"
							+ "src_count,start_time,tar_count,job_info,table_full_name,tactics_id) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, id);
					stmt.setString(2, tranfersDataBean.getBatch_id());
					stmt.setString(3, tranfersDataBean.getJob_id());
					stmt.setString(4, "running");
					stmt.setString(5, tranfersDataBean.getExec_type());
					stmt.setString(6, tranfersDataBean.getDb_name());
					stmt.setString(7, tranfersDataBean.getTab_name());
					stmt.setString(8, tranfersDataBean.getEtl_date());
					stmt.setInt(9, tranfersDataBean.getJob_src_count());
					stmt.setString(10, sdf.format(new Date()));
					stmt.setInt(11, tranfersDataBean.getJob_tar_count());
					stmt.setString(12, job_info);
					stmt.setString(13, tranfersDataBean.getTab_full_name());
					stmt.setString(14, tranfersDataBean.getTactics_id());
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("1".equals(type)) {//
					id=tranfersDataBean.getLog_id();
					String sql = "update etl_imp_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, sdf.format(new Date()));
					if(tranfersDataBean.getType()==3) {
						stmt.setString(3, "error1");
					}else {
						stmt.setString(3, "error");
					}
					stmt.setInt(4, tatol);
					stmt.setInt(5, tranfersDataBean.getJob_src_count());
					stmt.setInt(6, tranfersDataBean.getJob_tar_count());
					stmt.setString(7, id);
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("2".equals(type)) {
					id=tranfersDataBean.getLog_id();
					String sql = "update etl_imp_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, sdf.format(new Date()));
					stmt.setString(3, "success");
					stmt.setInt(4, tatol);
					stmt.setInt(5, tranfersDataBean.getJob_src_count());
					stmt.setInt(6, tranfersDataBean.getJob_tar_count());
					stmt.setString(7, id);
					stmt.addBatch();
					stmt.executeBatch();
				} else if ("3".equals(type)) {
					id=tranfersDataBean.getLog_id();
					String sql = "update etl_imp_job_log set job_info=?,task_status=? where id=?";
					stmt = conn_mysql.prepareStatement(sql);
					stmt.setString(1, job_info);
					stmt.setString(2, "running");
					stmt.setString(3, id);
					stmt.addBatch();
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
			} finally {
				try {
					stmt.close();
					conn_mysql.close();
				} catch (SQLException e) {
					String eStr = pubAPIs.getException(e);
					pubAPIs.writerLog(eStr);
				}
			}
		}
		
		
		public static  String[] checkfile(String file_path) {
       
			String[] list=null;
			File file =new File(file_path);
			if(file.isDirectory()) {
				 list=new File(file_path).list();
			}else {
				pubAPIs.writerErrorLog("文件路径存在问题：请核查路径:"+file_path);
			}
			return list;							
		}
		
		
		public boolean checktable(String table_name) {
			Connection conn_oracle = EtlDataPub.createOracleConnSM4("dataImport_oracle_login_ldap");
			int cc;
			try {
				String sql1="SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_NAME = UPPER('"+table_name +"')";
				System.out.println(sql1);
				Statement stmt1= conn_oracle.createStatement();
				ResultSet rs = stmt1.executeQuery(sql1);
				rs.next();
				cc=rs.getInt(1);
				if(cc==1) {
					return true;
				}else {
					return false;
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		}
 
		
			 
	
}


