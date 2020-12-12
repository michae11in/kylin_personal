package cn.com.pub;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

// ----------------------------------------------------------------------------------------------------------

public class PubAPIs {
	private Properties p = new Properties();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private String className = "";

	static private String logsPath = "logs";
	static private String errorLogsPath = "errorLogs";

	public void setClassName(String className) {
		this.className = className;
	}

	public static void setErrorLogsPath(String errorLogsPath) {
		PubAPIs.errorLogsPath = errorLogsPath;
	}

	public static void setLogsPath(String logsPath) {
		PubAPIs.logsPath = logsPath;
	}

	public static String getLogsPath() {
		return logsPath;
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

	public PubAPIs(String classNameT) {
		logsPath += getDirSign();
		if (classNameT == null || "".equals(classNameT.trim())) {
			System.out.println("ERROR: className is null or empty !");
			System.exit(-1);
		} else {
			LoadProfile("pubAPIs");
			className = classNameT;
			LoadProfile(classNameT);
		}
	}

	public Configuration getBaseConf() {
		Configuration conf = new Configuration();
		conf.addResource(new Path("extra_conf/core-site.xml"));
		conf.addResource(new Path("extra_conf/hdfs-site.xml"));
		conf.addResource(new Path("extra_conf/hbase-site.xml"));
		return conf;
	}

	public String getProperty(String paramKey) {
		String paramValue = p.getProperty(paramKey, "");
		return paramValue;
	}

	public String getProperty(String paramKey, String def) {
		String paramValue = p.getProperty(paramKey, def);
		return paramValue;
	}

	public String getProperty2(String paramKey1, String paramKey2, String split1, String split2) {
		String paramValue = "";
		String paramValue1 = p.getProperty(paramKey1, "");
		String paramStr2[] = paramValue1.split(split1);
		for (int i = 0; i < paramStr2.length; i++) {
			String tmp = paramStr2[i];
			if (paramKey2.equals(tmp.split(split2)[0]))
				paramValue = tmp.split(split2)[1];
		}
		return paramValue;
	}

	public boolean hasDirSign(String filePath) {
		boolean result = false;
		if (filePath.lastIndexOf("/") == (filePath.length() - 1)) {
			result = true;
		}
		if (filePath.lastIndexOf("\\") == (filePath.length() - 1)) {
			result = true;
		}
		return result;
	}

	private String extra_log = "";

	public void setExtra_log(String extra_log) {
		this.extra_log = extra_log;
	}

	public String getException(Exception e) {
		StringWriter sw = new StringWriter();
		try (PrintWriter pw = new PrintWriter(sw);) {
			e.printStackTrace(pw);
		}
		String errorInfo = sw.toString();
		return errorInfo;
	}

	public void writerLog(String logStr) {
		try {
			Properties props = System.getProperties(); // 获得系统属性集
			String osName1 = props.getProperty("os.name"); // 操作系统名称
			String dirSign = "";
			if (osName1.indexOf("Windows") != -1) {
				dirSign = "\\";
			} else {
				dirSign = "/";
			}
			File file = new File(logsPath + dirSign);
			if (!file.exists()) {
				file.mkdirs();
			}
			String filePath = logsPath + className + ".log";
			String filePath_bak = logsPath + className + ".log.bak";
			File logfile = new File(filePath);
			if (!logfile.exists()) {
				logfile.createNewFile();
			}
			long fileSize = logfile.length();
			if (fileSize > (1024 * 1024 * 64)) {// 存64M
				File logfilebak = new File(filePath_bak);
				if (logfilebak.exists()) {
					logfilebak.delete();
				}
				logfile.renameTo(logfilebak);

				logfile = new File(filePath);
				logfile.createNewFile();
			}
			FileWriter fw = new FileWriter(logfile, true);
			PrintWriter pw = new PrintWriter(fw);
			String runTime = sdf.format(new Date());
			long thr_id = Thread.currentThread().getId();
			thr_id = 100000 + thr_id;
			String log_str = runTime + " Thread-" + thr_id + ": " + extra_log + logStr;
			System.out.println(log_str);
			pw.println(log_str);
			pw.flush();
			pw.close();
			fw.close();
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public void writerErrorLog(String logStr) {
		writerLog(logStr);
		try {
			Properties props = System.getProperties(); // 获得系统属性集
			String osName1 = props.getProperty("os.name"); // 操作系统名称
			String dirSign = "";
			if (osName1.indexOf("Windows") != -1) {
				dirSign = "\\";
			} else {
				dirSign = "/";
			}
			File file = new File(errorLogsPath + dirSign);
			if (!file.exists()) {
				file.mkdirs();
			}
			String filePath = errorLogsPath + className + ".error";
			String filePath_bak = errorLogsPath + className + ".error.bak";
			File logfile = new File(filePath);
			if (!logfile.exists()) {
				logfile.createNewFile();
			}
			long fileSize = logfile.length();
			if (fileSize > (1024 * 1024 * 64)) {// 存64M
				File logfilebak = new File(filePath_bak);
				if (logfilebak.exists()) {
					logfilebak.delete();
				}
				logfile.renameTo(logfilebak);

				logfile = new File(filePath);
				logfile.createNewFile();
			}
			FileWriter fw = new FileWriter(logfile, true);
			PrintWriter pw = new PrintWriter(fw);
			String runTime = sdf.format(new Date());
			long thr_id = Thread.currentThread().getId();
			thr_id = 100000 + thr_id;
			String log_str = runTime + " Thread-" + thr_id + ": " + extra_log + logStr;
			pw.println(log_str);
			pw.flush();
			pw.close();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static synchronized Connection createTDHConn(String authUser) {
		Connection conn = null;
		try {
			PubAPIs pubAPIs = new PubAPIs("pubAPIs");
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

			e.printStackTrace();
			return conn;
		}
		return conn;
	}

	public static synchronized Connection createTDHConnSM4(String authUser) {
		Connection conn = null;
		try {
			PubAPIs pubAPIs = new PubAPIs("pubAPIs");
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

			e.printStackTrace();
			return conn;
		}
		return conn;
	}

	public static synchronized Connection createMysqlConn(String authUser) {
		Connection conn = null;
		try {
			PubAPIs pubAPIs = new PubAPIs("pubAPIs");
			Class.forName("com.mysql.jdbc.Driver");
			String db_mysqlAddr = pubAPIs.getProperty2(authUser, "db_mysqlAddr", "`", ";");
			String db_user_id = pubAPIs.getProperty2(authUser, "db_user_id", "`", ";");
			String db_user_pwd = pubAPIs.getProperty2(authUser, "db_user_pwd", "`", ";");
			// db_user_pwd = getPassText(db_user_pwd);
			String urlStr = "jdbc:mysql://" + db_mysqlAddr + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
			conn = DriverManager.getConnection(urlStr, db_user_id, db_user_pwd);

		} catch (Exception e) {

			e.printStackTrace();
			return conn;
		}
		return conn;
	}

	public static synchronized Connection createMysqlConnSM4(String authUser) {
		Connection conn = null;
		try {
			PubAPIs pubAPIs = new PubAPIs("pubAPIs");
			Class.forName("com.mysql.jdbc.Driver");
			String db_mysqlAddr = pubAPIs.getProperty2(authUser, "db_mysqlAddr", "`", ";");
			String db_user_id = pubAPIs.getProperty2(authUser, "db_user_id", "`", ";");
			String db_user_pwd = pubAPIs.getProperty2(authUser, "db_user_pwd", "`", ";");
			db_user_pwd = pubAPIs.getPassText(db_user_pwd);
			String urlStr = "jdbc:mysql://" + db_mysqlAddr + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
			conn = DriverManager.getConnection(urlStr, db_user_id, db_user_pwd);

		} catch (Exception e) {

			e.printStackTrace();
			return conn;
		}
		return conn;
	}


	public void LoadProfile(String filePath) {
		className = filePath;
		try {
			File file = new File("profile/" + filePath + ".properties");
			if (file.isFile()) {
				p.load(new FileInputStream(file));
			}
		} catch (Exception e) {

			e.printStackTrace();
			System.exit(-1);
		}
	}

	public String getPassText(String db_user_pwd) {
		String[] db_user_pwds = db_user_pwd.split("\\|");
		byte[] keytes = new byte[db_user_pwds.length];
		for (int i = 0; i < db_user_pwds.length; i++) {
			String tmp = db_user_pwds[i];
			keytes[i] = (byte) Integer.parseInt(tmp);
		}
		PubSMS sms = new PubSMS();
		byte[] deOut = sms.decodeSMS4(keytes, sms.key);
		String deOutStr = new String(deOut);
		deOutStr = deOutStr.substring(0, deOutStr.indexOf("|"));
		return deOutStr;
	}

	/*private static Configuration conf1 = null;

	public static synchronized FileSystem getFileSystem(String conf_hdfs, String login_kerberos, String classname,
			String type, FileSystem fs1) throws Exception {
		if ("get".equals(type)) {
			PubAPIs pubAPIs = new PubAPIs("pubAPIs");
			if (!"".equals(classname)) {
				pubAPIs.LoadProfile(classname);
			}
			String login_type = pubAPIs.getProperty2(login_kerberos, "type", "`", ";");
			String login_krb5conf = pubAPIs.getProperty2(login_kerberos, "krb5conf", "`", ";");
			String login_username = pubAPIs.getProperty2(login_kerberos, "username", "`", ";");
			String login_password = pubAPIs.getProperty2(login_kerberos, "password", "`", ";");
			login_password = pubAPIs.getPassText(login_password);
			String l_nameNodeIPnn1 = "";
			String l_nameNodeIPnn2 = "";
			String l_nameNodeIP = pubAPIs.getProperty2(conf_hdfs, "nameNodeIP", "`", ";");
			if (!"".equals(l_nameNodeIP) && l_nameNodeIP.indexOf(",") != -1) {
				l_nameNodeIPnn1 = l_nameNodeIP != null ? l_nameNodeIP.split(",")[0] : "";
				l_nameNodeIPnn2 = l_nameNodeIP != null ? l_nameNodeIP.split(",")[1] : "";
			}
			conf1 = pubAPIs.getBaseConf();
			conf1.set("dfs.namenode.rpc-address.nameservice1.nn1", l_nameNodeIPnn1);
			conf1.set("dfs.namenode.rpc-address.nameservice1.nn2", l_nameNodeIPnn2);

			FileSystem fs = null;
			if ("keytab".equals(login_type)) {
				System.setProperty("java.security.krb5.conf", login_krb5conf);
				UserGroupInformation.setConfiguration(conf1);
				UserGroupInformation app_ugi = UserGroupInformation.loginUserFromPasswordAndReturnUGI(login_username,
						login_password);
				fs = app_ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
					public FileSystem run() throws Exception {
						// FileSystem fs = FileSystem.newInstance(conf1);
						FileSystem fs = FileSystem.get(conf1);
						fs.getStatus();
						return fs;
					}
				});
			} else {
				System.setProperty("HADOOP_USER_NAME", login_username);
				fs = FileSystem.get(conf1);
				fs.getStatus();
			}
			return fs;
		} else if ("close".equals(type)) {
			if (fs1 != null) {
				fs1.close();
			}
			return null;
		} else {
			return null;
		}
	}*/

	private static Configuration conf2 = null;
	public static synchronized FileSystem getFileSystem(String conf_hdfs, String login_kerberos, String classname,
			String type, FileSystem fs1) throws Exception {
		if ("get".equals(type)) {
			PubAPIs pubAPIs = new PubAPIs("pubAPIs");
			if (!"".equals(classname)) {
				pubAPIs.LoadProfile(classname);
			}
			FileSystem fs = null;
			String conf_path = pubAPIs.getProperty2(conf_hdfs, "conf_path", "`", ";");
			conf2 = new Configuration();
			conf2.setBoolean("fs.hdfs.impl.disable.cache", true);
			conf2.addResource(new Path(conf_path + "/core-site.xml"));
			conf2.addResource(new Path(conf_path + "/hdfs-site.xml"));
			System.setProperty("java.security.krb5.conf", conf_path + "/krb5.conf");
			UserGroupInformation.setConfiguration(conf2);
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
					FileSystem fs = FileSystem.newInstance(conf2);
					return fs;
				}
			});
			return fs;
		} else if ("close".equals(type)) {
			if (fs1 != null) {
				fs1.close();
			}
			return null;
		} else {
			return null;
		}
	}

	public int sms4(byte[] in, int inLen, byte[] key, byte[] out, int CryptFlag) {
		PubSMS sms = new PubSMS();
		return sms.sms4(in, inLen, key, out, CryptFlag);
	}

	public String getMD5(InputStream in) {
		String md5Str = "";
		try {
			MessageDigest mMessageDigest = MessageDigest.getInstance("MD5");
			byte[] buffer = new byte[3072];
			int length = -1;
			while ((length = in.read(buffer, 0, 3072)) > 0) {
				mMessageDigest.update(buffer, 0, length);
			}
			in.close();
			md5Str = new BigInteger(1, mMessageDigest.digest()).toString(16);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return md5Str;
	}

	public static void main(String[] args) {
		String newString = "abcd1234|";
		String pwd = "";
		if (args.length == 1) {
			newString = args[0];
		}
		PubSMS sms = new PubSMS();
		byte[] enOut = sms.encodeSMS4(newString, sms.key);
		if (enOut == null) {
			return;
		}
		Integer.toHexString(1);
		byte[] keytes = new byte[enOut.length];
		for (int i = 0; i < enOut.length; i++) {
			String tmp = enOut[i] + "";
			pwd += tmp + "|";
			// System.out.print(tmp + "|");
			keytes[i] = (byte) Integer.parseInt(tmp);
		}
		// System.out.println("\n");
		new PubAPIs("PubAPIs").writerLog("\n" + newString + "加密后密码为：" + pwd);
		byte[] deOut = sms.decodeSMS4(keytes, sms.key);
		String deOutStr = new String(deOut);
		new PubAPIs("PubAPIs").writerLog("\n解密结果(return byte[])：" + deOutStr.substring(0, deOutStr.indexOf("|")));
	}
}
