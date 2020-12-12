package cn.com.deleteHistFiles;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.com.dataHandle.DataLoadPub;
import cn.com.pub.PubAPIs;

public class deleteHistFileScheduler {
	private PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private Connection conn_mysql = null;
	private String authUserMysql = "";
	private String conf_hdfs = "";
	private String login_kerberos = "";
	private String dataFile_curr_dir = "";
	private String dataFile_hist_dir = "";
	private String home_path = "";
	private String log_file_dir="";
	private int delNum=7;
	private void deleteHistory(String tactics_id1, String tactics_id2) throws Exception {
		authUserMysql = "dataLoad_mysql_login_ldap";
		conn_mysql = DataLoadPub.createMysqlConn(authUserMysql);
		// hdfs
		Statement stmt0 = conn_mysql.createStatement();
		String sql0 = "select data_file_curr_dir,data_file_hist_dir,log_file_dir,login_ldap,conf_hdfs,login_kerberos from dataload_prop_info where tactics_id = '"
				+ tactics_id1 + "'";
		pubAPIs.writerLog("sql0:" + sql0);
		ResultSet rs0 = stmt0.executeQuery(sql0);
		while (rs0.next()) {
			log_file_dir=rs0.getString("log_file_dir").trim();
			dataFile_curr_dir = rs0.getString("data_file_curr_dir").trim();
			dataFile_hist_dir = rs0.getString("data_file_hist_dir").trim();
			conf_hdfs = rs0.getString("conf_hdfs").trim();
			login_kerberos = rs0.getString("login_kerberos").trim();
		}
		rs0.close();
		stmt0.close();
		if (log_file_dir == null || "".equals(log_file_dir.trim())) {
			log_file_dir = "logs/";
		}
		String lastChar = log_file_dir.substring(log_file_dir.length() - 1, log_file_dir.length());
		if ("\\,/".indexOf(lastChar) == -1 ) {
			log_file_dir=log_file_dir+PubAPIs.getDirSign();
		}
		PubAPIs.setLogsPath(log_file_dir);
		// local
		Statement stmt1 = conn_mysql.createStatement();
		String sql1 = "select home_path,log_file_dir from transfer_exp_prop_info where tactics_id = '" + tactics_id2
				+ "';";
		pubAPIs.writerLog("sql0:" + sql0);
		ResultSet rs1 = stmt1.executeQuery(sql1);
		while (rs1.next()) {
			home_path = rs1.getString("home_path").trim();
		}
		rs1.close();
		stmt1.close();

		// HDFS上文件目录
		String datafile_hist_dir = dataFile_hist_dir;
		String datafile_curr_dir = dataFile_curr_dir;
		FileSystem fs = DataLoadPub.getFileSystem(conf_hdfs, login_kerberos);
		if(datafile_hist_dir!=null&&!datafile_hist_dir.equals("")) {
			pubAPIs.writerLog("开始刪除hdfs上历史文件目录:" );
			removeHistoryDirHDFS(fs, datafile_hist_dir);
		}else {
			pubAPIs.writerLog("获取到的hdfs上历史文件目录为空：文件目录未删除" );
		}
		if(datafile_curr_dir!=null&&!datafile_curr_dir.equals("")) {
			pubAPIs.writerLog("开始刪除hdfs上推送文件目录:" );
			removeHistoryDirHDFS(fs, datafile_curr_dir);
		}else {
			pubAPIs.writerLog("获取到的hdfs上推送文件目录为空：文件目录未删除" );
		}
			
		// 本地文件目录
		String home_path_local = home_path;	
		if(home_path_local!=null&&!home_path_local.equals("")) {
			removeHistoryDirLocal(home_path_local);
		}else {
			pubAPIs.writerLog("获取本地落地文件目录为空：文件目录未删除" );
		}
		
		pubAPIs.writerLog("刪除文件结束:" );
	}

	private boolean removeHistoryDirHDFS(FileSystem fs, String fileStr) throws Exception {
		String property = pubAPIs.getProperty("local_delNum");
		if(property!=null&&!property.equals(property))		delNum=Integer.parseInt(property);
		boolean flg = false;
		FileStatus[] files = null;
		List<FileStatus> list = new ArrayList<>();
		files = fs.listStatus(new Path(fileStr));
		for (FileStatus f : files) {
			list.add(f);
		}
		if (files.length > 0) {
			Collections.sort(list, new Comparator<FileStatus>() {
				@Override
				public int compare(FileStatus o1, FileStatus o2) {
					if (o1.getModificationTime() < o2.getModificationTime()) {
						return 1;
					} else if (o1.getModificationTime() == o2.getModificationTime()) {
						return 0;
					}
					return -1;
				}
			});
		}
		for (int i = 0; i < list.size(); i++) {
			if (i >= delNum) { // 文件保留个数
				String delFile = list.get(i).getPath().toUri().getPath();
				flg = fs.delete(new Path(delFile), true);
				pubAPIs.writerLog("刪除:"+delFile );
			}
		}
		return flg;
	}

	private void removeHistoryDirLocal(String fileStr) throws Exception {
		String property = pubAPIs.getProperty("HDFS_delNum");
		if(property!=null&&!property.equals("")) delNum=Integer.parseInt(property);
		File realFile = new File(fileStr);
		File[] subfiles = realFile.listFiles();
		List<File> list = new ArrayList<File>();
		for (File file : subfiles) {
			list.add(file);
		}
		if (list != null && list.size() > 0) {
			Collections.sort(list, new Comparator<File>() {
				public int compare(File file, File newFile) {
					if (file.lastModified() < newFile.lastModified()) {// 降序<;升序>
						return 1;
					} else if (file.lastModified() == newFile.lastModified()) {
						return 0;
					} else {
						return -1;
					}
				}
			});
		}
		for (int i = 0; i < list.size(); i++) {
			if (i >= delNum) { // 文件保留个数
				String dirPath = list.get(i).getPath().toString();
				deleteDir(dirPath);
				pubAPIs.writerLog("刪除:"+dirPath );
			}
		}
	}

	private static void deleteDir(String dirPath) {
		File file = new File(dirPath);
		if (file.isFile()) {
			file.delete();
		} else {
			File[] files = file.listFiles();
			if (files == null) {
				file.delete();
			} else {
				for (int i = 0; i < files.length; i++) {
					deleteDir(files[i].getAbsolutePath());
				}
				file.delete();
			}
		}
	}

	public static void main(String[] args) {
		String tactics_id1 = "debug";//删除推送的hdfs上的文件目录
		String tactics_id2 = "002";//删除落地文件目录
		if(args.length==2) {
			tactics_id1=args[0];
			tactics_id2=args[1];
		}else {
			
		}
		deleteHistFileScheduler deleteHistFileScheduler = new deleteHistFileScheduler();
		try {
			deleteHistFileScheduler.deleteHistory(tactics_id1, tactics_id2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
