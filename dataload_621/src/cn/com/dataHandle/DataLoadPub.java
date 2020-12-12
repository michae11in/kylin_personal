package cn.com.dataHandle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import cn.com.dataHandle.bean.DataLoadBean;
import cn.com.pub.PubAPIs;

public class DataLoadPub {

	private static PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private static HashMap<String, Integer> threadhashMap = new HashMap<String, Integer>();

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

	public static synchronized int threadLock(String lockObject, String type, String sou_tab, int lock_table_num) {
		Integer tmp = threadhashMap.get(lockObject);
		int thread_num = tmp != null ? tmp : 0;
		if ("add".equals(type)) {
			thread_num++;
			threadhashMap.put(lockObject, thread_num);
		} else if ("minus".equals(type)) {
			thread_num--;
			threadhashMap.put(lockObject, thread_num);
		} else if ("del".equals(type)) {
			threadhashMap.remove(lockObject);
		} else if ("check".equals(type)) {
			if (thread_num < lock_table_num) {// -1表达得到了跑权限
				thread_num++;
				threadhashMap.put(lockObject, thread_num);
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

	public static synchronized void addStringToFile(String file_path, String info, String lastChar) throws Exception {
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
		pubAPIs.writerLog(file_path + ":\n" + info);
		ps.close();
	}

	public int getProcessID() {
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		return Integer.valueOf(runtimeMXBean.getName().split("@")[0]).intValue();
	}

	public static String getTable_columns_txt(String table_columns_txt, String etl_date, String sys_name) {
		Pattern P = Pattern.compile("^\\$\\{(\\w*|\\W*|[\\u4e00-\\u9fa5])\\}");
		Matcher m = P.matcher(table_columns_txt);
		boolean matches = m.matches();
		if (matches == true) {
			if (table_columns_txt.equals("${bucket_id}")) {
				table_columns_txt = "uniq()";
				return table_columns_txt;
			}
			if (table_columns_txt.equals("${etl_date}")) {
				table_columns_txt = "'" + etl_date + "'";
				return table_columns_txt;
			}
			if (table_columns_txt.equals("${system_id}")) {
				table_columns_txt = "'" + sys_name + "'";
				return table_columns_txt;
			}

		}
		return table_columns_txt;
	}

	private static Configuration conf = null;
	public static synchronized FileSystem getFileSystem(String conf_hdfs, String login_kerberos) throws Exception {
		FileSystem fs = null;
		String conf_path = pubAPIs.getProperty2(conf_hdfs, "conf_path", "`", ";");
		conf = new Configuration();
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		conf.addResource(new Path(conf_path + "/core-site.xml"));
		conf.addResource(new Path(conf_path + "/hdfs-site.xml"));
		System.setProperty("java.security.krb5.conf", conf_path + "/krb5.conf");
		UserGroupInformation.setConfiguration(conf);
		String login_type = pubAPIs.getProperty2(login_kerberos, "type", "`", ";");
		String user_name = pubAPIs.getProperty2(login_kerberos, "username", "`", ";");
		UserGroupInformation app_ugi = null;
		if ("keytab".equals(login_type)) {
			app_ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user_name, login_type);
		} else {
			String login_password = pubAPIs.getProperty2(login_kerberos, "password", "`", ";");
			login_password = pubAPIs.getPassText(login_password);
			app_ugi = UserGroupInformation.loginUserFromPasswordAndReturnUGI(user_name, login_password);
		}
		
		fs = app_ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
			public FileSystem run() throws Exception {
				FileSystem fs = FileSystem.newInstance(conf);
				return fs;
			}
		});
		return fs;
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
            String characterEncoding = "";
    	    characterEncoding = pubAPIs.getProperty("characterEncoding");
			String urlStr = "jdbc:hive2://" + db_inceptorAddr + "/default";
			if ("none".equals(authType)) {
				conn = DriverManager.getConnection(urlStr);
			} else if ("ldap".equals(authType)) {
				urlStr = "jdbc:hive2://" + db_inceptorAddr + "/default"+characterEncoding;
				pubAPIs.writerLog("urlStr---->"+urlStr);
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

	// 判断end文件是否存在
	public static synchronized boolean checkENDFile(DataLoadBean dataLoadBean, FileSystem fs) throws Exception {
		return fs.exists(new Path(dataLoadBean.getDatafile_curr_end_file()));
	}

	// 判断end文件是否存在
	public static synchronized boolean checkHistENDFile(DataLoadBean dataLoadBean, FileSystem fs) throws Exception {
		return fs.exists(new Path(dataLoadBean.getDatafile_hist_end_file()));
	}

	// 判断SQL文件是否存在
	public static synchronized boolean checkSQLFile(DataLoadBean dataLoadBean, FileSystem fs) throws Exception {
		return fs.exists(new Path(dataLoadBean.getDatafile_curr_sql_file()));
	}

	// 判断数据文件是否存在
	public static synchronized boolean checkTXTFile(DataLoadBean dataLoadBean, FileSystem fs) throws Exception {
		return fs.exists(new Path(dataLoadBean.getDatafile_curr_txt_file()));
	}

	// 得到历史目录中txt文件的大小
	public static synchronized long getFileSize(String file_path, FileSystem fs) throws Exception {
		return fs.getContentSummary(new Path(file_path)).getLength();
	}

	public static synchronized void moveENDFile(DataLoadBean dataLoadBean, FileSystem fs) throws Exception {
		Path src = new Path(dataLoadBean.getDatafile_curr_end_file());
		Path dst = new Path(dataLoadBean.getDatafile_hist_end_file());
		if (fs.exists(dst)) {
			fs.delete(dst, false);// 不递归删除
		} else {
			fs.mkdirs(dst.getParent());
		}
		fs.rename(src, dst);
	}

	public static synchronized void moveSQLFile(DataLoadBean dataLoadBean, FileSystem fs) throws Exception {
		Path src = new Path(dataLoadBean.getDatafile_curr_sql_file());
		Path dst = new Path(dataLoadBean.getDatafile_hist_sql_file());
		if (fs.exists(dst)) {
			fs.delete(dst, false);// 不递归删除
		} else {
			fs.mkdirs(dst.getParent());
		}
		fs.rename(src, dst);
	}

	public static synchronized void moveTXTFile(DataLoadBean dataLoadBean, FileSystem fs) throws Exception {
		Path src = new Path(dataLoadBean.getDatafile_curr_txt_file());
		Path dst = new Path(dataLoadBean.getDatafile_hist_txt_file());
		if (fs.exists(dst)) {
			fs.delete(dst, false);// 不递归删除
		} else {
			fs.mkdirs(dst.getParent());
		}
		fs.rename(src, dst);
	}

	public static synchronized void write_jobLog(DataLoadBean dataLoadBean, String type, String job_info) {
		PreparedStatement stmt = null;
		Connection conn_mysql = null;
		long endTime = new Date().getTime();
		int tatol = (int) ((endTime - dataLoadBean.getStartTime()) * 1 / 1000);
		String id = "" ;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if (job_info.length() > 5000) {
			pubAPIs.writerLog("job_info too log:" + job_info);
			job_info = job_info.substring(0, 5000);
		}
		try {
			conn_mysql = createMysqlConnSM4(dataLoadBean.getAuthUserMysql());
			if ("0".equals(type)) {// 开始
/*				String sql = "delete from dataload_job_log where table_name =? and etl_date = ?";
				stmt = conn_mysql.prepareStatement(sql);
				stmt.setString(1, dataLoadBean.getTable_name());
				stmt.setString(2, dataLoadBean.getEtl_date());
				stmt.addBatch();
				stmt.executeBatch();
				stmt.close();*/
				String start_time =sdf.format(new Date());
				id=dataLoadBean.getJob_id() + "\001"+start_time.replace("-", "").replace(":", "").replace(" ", "\001");
				dataLoadBean.setLog_id(id);
				String sql_i = "insert into dataload_job_log(id,batch_id,job_id,task_status,src_sys_id,src_table_type,table_full_name_orc,src_file_name,etl_date,"
						+ "src_count,start_time,used_time,tar_count,job_info,table_name,tactics_id) "
						+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				stmt = conn_mysql.prepareStatement(sql_i);
				stmt.setString(1, id);
				stmt.setString(2, dataLoadBean.getBatch_id());
				stmt.setString(3, dataLoadBean.getJob_id());
				stmt.setString(4, "running");
				stmt.setString(5, dataLoadBean.getSrc_sys_id());
				stmt.setString(6, dataLoadBean.getSrc_table_type());
				stmt.setString(7, dataLoadBean.getTable_full_name_orc());
				stmt.setString(8, dataLoadBean.getSrc_file_name());
				stmt.setString(9, dataLoadBean.getEtl_date());
				stmt.setInt(10, dataLoadBean.getJob_src_count());
				stmt.setString(11, sdf.format(new Date()));
				stmt.setInt(12, tatol);
				stmt.setInt(13, dataLoadBean.getJob_tar_count());
				stmt.setString(14, job_info);
				stmt.setString(15, dataLoadBean.getTable_name());
				stmt.setString(16, dataLoadBean.getTactics_id());
				stmt.addBatch();
				stmt.executeBatch();
			} else if ("1".equals(type)) {//
				id = dataLoadBean.getLog_id();
				String sql = "update dataload_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
				stmt = conn_mysql.prepareStatement(sql);
				stmt.setString(1, job_info);
				stmt.setString(2, sdf.format(new Date()));
				if(dataLoadBean.getType()==3) {
					stmt.setString(3, "error1");
				}else {
					stmt.setString(3, "error");
				}
				stmt.setInt(4, tatol);
				stmt.setInt(5, dataLoadBean.getJob_src_count());
				stmt.setInt(6, dataLoadBean.getJob_tar_count());
				stmt.setString(7, id);
				stmt.addBatch();
				stmt.executeBatch();
			} else if ("2".equals(type)) {
				id = dataLoadBean.getLog_id();
				String sql = "update dataload_job_log set job_info=?,end_time=?,task_status=?,used_time=?,src_count=?,tar_count=? where id=?";
				stmt = conn_mysql.prepareStatement(sql);
				stmt.setString(1, job_info);
				stmt.setString(2, sdf.format(new Date()));
				stmt.setString(3, "success");
				stmt.setInt(4, tatol);
				stmt.setInt(5, dataLoadBean.getJob_src_count());
				stmt.setInt(6, dataLoadBean.getJob_tar_count());
				stmt.setString(7, id);
				stmt.addBatch();
				stmt.executeBatch();
			} else if ("3".equals(type)) {
				id = dataLoadBean.getLog_id();
				String sql = "update dataload_job_log set job_info=?,task_status=? where id=?";
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

	public static synchronized void write_warnInfo(DataLoadBean dataLoadBean, String warn_type, String warn_level,
			String warn_info) {
		PreparedStatement stmt = null;
		Connection conn_mysql = null;
		String id = UUID.randomUUID().toString().replace("-", "").replace(".", "");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			conn_mysql = createMysqlConnSM4(dataLoadBean.getAuthUserMysql());
			String warnInfosql = "insert into dataload_warn_info(warn_id,warn_type,warn_level,warn_time,tab_full_name,warn_info) values(?,?,?,?,?,?)";
			pubAPIs.writerLog("warnInfosql:" + warnInfosql);
			stmt = conn_mysql.prepareStatement(warnInfosql);
			stmt.setString(1, id);
			stmt.setString(2, warn_type);
			stmt.setString(3, warn_level);
			stmt.setString(4, sdf.format(new Date()));
			stmt.setString(5, dataLoadBean.getTable_full_name_orc());
			stmt.setString(6, warn_info);
			stmt.addBatch();
			stmt.executeBatch();
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			pubAPIs.writerLog(eStr);
		} finally {
			try {
				stmt.close();
				conn_mysql.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static synchronized int getSrcTabSize(DataLoadBean dataLoadBean) throws Exception {
		FileSystem fs = DataLoadPub.getFileSystem(dataLoadBean.getConf_hdfs(), dataLoadBean.getLogin_kerberos());
		Path p = new Path(dataLoadBean.getDatafile_hist_end_file());
		FSDataInputStream fsr = fs.open(p);
		BufferedReader br = new BufferedReader(new InputStreamReader(fsr));
		String tempStr;
		StringBuffer sbf = new StringBuffer();
		while ((tempStr = br.readLine()) != null) {
			sbf.append(tempStr);
		}
		String tmp = sbf.toString().trim();
		br.close();
		if ("".equals(tmp)) {
			tmp = "-1";
		}
		return Integer.parseInt(tmp);
	}

}
