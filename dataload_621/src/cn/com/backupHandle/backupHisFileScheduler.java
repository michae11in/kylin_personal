package cn.com.backupHandle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.fs.FileSystem;

import cn.com.dataHandle.DataLoadMain;
import cn.com.dataHandle.DataLoadPub;
import cn.com.dataHandle.DataLoadScheduler;
import cn.com.dataHandle.bean.DataLoadBean;
import cn.com.pub.PubAPIs;
import cn.com.pub.PubException;

public class backupHisFileScheduler {

	private PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private String thread_id = UUID.randomUUID().toString().replace("-", "").replace(".", "");
	private Connection conn_mysql = null;
	private String authUserTDH = "";
	private String authUserMysql = "";
	private String conf_hdfs = "";
	private String login_kerberos = "";
	private int parallSpotInt = 5;
	private int reRun = 3;
	private String dataFile_curr_dir = "";
	private String dataFile_hist_dir = "";
	private String job_ids = "";
	private String delimited = "";
	private String log_file_dir = "";
	private HashMap<String, HashMap<String,String>> columnMap = new HashMap<String,  HashMap<String,String>>();

	public void setJob_ids(String job_ids) {
		this.job_ids = job_ids;
	}
	

	public int runByBatch_ids(String tactics_id, String etl_date, String type) {
		int retNum=0;
		try {
			if ("2".equals(type) && "".equals(job_ids.trim())) {
				throw new PubException("传入参数不正确:当type为2时job_ids值不能为空，必须是job_id的拼接，多个job_id间用,分开");
			}
			if (",0,1,2,3,".indexOf("," + type + ",") == -1) {
				throw new PubException("type值必须为：0，1，2");
			}
			authUserMysql = "dataLoad_mysql_login_ldap";
			conn_mysql = DataLoadPub.createMysqlConn(authUserMysql);
			Statement stmt0 = conn_mysql.createStatement();
			String sql0 = "select data_file_curr_dir,data_file_hist_dir,log_file_dir,login_ldap,conf_hdfs,login_kerberos,txt_delimited,parall_spot,re_run from dataload_prop_info where tactics_id = '"
					+ tactics_id + "'";
			pubAPIs.writerLog("sql0:" + sql0);
			ResultSet rs0 = stmt0.executeQuery(sql0);
			while (rs0.next()) {
				dataFile_curr_dir = rs0.getString("data_file_curr_dir").trim();
				dataFile_hist_dir = rs0.getString("data_file_hist_dir").trim();
				log_file_dir = rs0.getString("log_file_dir").trim();
				authUserTDH = rs0.getString("login_ldap").trim();
				conf_hdfs = rs0.getString("conf_hdfs").trim();
				login_kerberos = rs0.getString("login_kerberos").trim();
				delimited = rs0.getString("txt_delimited").trim();
				parallSpotInt = rs0.getInt("parall_spot");
				reRun = rs0.getInt("re_run");
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

			Statement stmt1 = conn_mysql.createStatement();
			String sql1 = "select src_column,tdh_column,src_type from datahandle_column_map order by src_type";
			pubAPIs.writerLog("sql1:" + sql1);
			ResultSet rs1 = stmt1.executeQuery(sql1);
			HashMap<String, String> column_mapping_map = new HashMap<String,String>();
			String src_type_bef = "";
			while (rs1.next()) {
				String src_type = rs1.getString("src_type").trim().toLowerCase();
				String src_column = rs1.getString("src_column").trim().toLowerCase();
				String tdh_column = rs1.getString("tdh_column").trim().toLowerCase();
				if (!src_type.equals(src_type_bef)) {
					column_mapping_map.put(src_column,tdh_column);
					src_type_bef = src_type;
					if (",oracle,".indexOf("," + src_type + ",") == -1) {
						throw new PubException("column_mapping表的src_type字段值为" + src_type + "不符合预期");
					}
				} else if (!"".equals(src_type_bef)) {
					column_mapping_map.put(src_column,tdh_column);
				}
				columnMap.put(src_type_bef, column_mapping_map);
			}
			rs1.close();
			stmt1.close();
			conn_mysql.close();

			while (true) {
				int result = runByBatch_id(tactics_id, etl_date, type);
				Calendar cal = Calendar.getInstance();
				int hour = cal.get(Calendar.HOUR_OF_DAY);
				if (result == 0 || hour >= 23) {// 超过晚上11点不再跑当天批次
					break;
				} else {
					Thread.sleep(10000);
				}
			}
			String sql2 = "select job_id,table_name from dataload_tactics_info t where t.enabled = '1' and t.tactics_id='" + tactics_id
					        +"'and not exists (select 1 from dataload_job_log t1 where t1.table_name = t.table_name and t1.etl_date= '"+etl_date+ "'and t1.task_status is not null and t1.task_status<>'running');";
			pubAPIs.writerLog("if_running:"+sql2);
			boolean flg =true;
			ResultSet rs2 =null;
			while(flg) {
				String  strTmp=""; 
				Connection conn_mysq2 = DataLoadPub.createMysqlConn(authUserMysql);
				Statement stmt2 = conn_mysq2.createStatement();
			    rs2 = stmt2.executeQuery(sql2);
				flg=false;
				while (rs2.next()) {
					flg=true;
					strTmp += rs2.getString("table_name")+",";
				}
				rs2.close();
				stmt2.close();
				conn_mysq2.close();
				pubAPIs.writerLog(tactics_id+"：等待"+strTmp+"表加载数据！");
				Thread.sleep(20000);
			}
			if(type.equals("3")) {
				String check_sql="select job_id from dataload_tactics_info t where t.enabled = '1' and t.tactics_id='" + tactics_id
						         +"' and  exists (select 1 from dataload_job_log t1 where t1.table_name = t.table_name and t1.etl_date= '"+etl_date+ "'and t1.task_status ='error1');";
				Connection conn_mysq3 = DataLoadPub.createMysqlConn(authUserMysql);
				Statement stmt3 = conn_mysq3.createStatement();
				ResultSet rs3 = stmt3.executeQuery(check_sql);
				while (rs3.next()) {
					retNum=-1;
				}
				rs3.close();
				stmt3.close();
				conn_mysq3.close();
			}
			pubAPIs.writerLog(tactics_id+"批次 数据加载结束");
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			pubAPIs.writerLog(eStr);
		} finally {
			try {
				if (conn_mysql != null && !conn_mysql.isClosed()) {
					conn_mysql.close();
				}
			} catch (SQLException e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
			}
		}
		return retNum;

	}

	public int runByBatch_id(String tactics_id, String etl_date, String type) throws Exception {
		int flg=-1;
		int result = 0;
		int m = 0;
		if (DataLoadPub.threadLock(thread_id, "check", "schedulerThread", parallSpotInt) != -1) {// 如果上一轮所有作业都没有跑完
			return -1;
		}
		conn_mysql = DataLoadPub.createMysqlConn(authUserMysql);
		// 从配置表读取每个表的配置信息
		ArrayList<DataLoadBean> dataLoadBeanList = new ArrayList<DataLoadBean>();
		Statement stmt1 = conn_mysql.createStatement();
		String sql1 = "select job_id,tactics_id,src_sys_id,src_table_type,src_file_name,db_name_txt,db_name_orc,table_name,load_condition,load_type,col_map_type,usr_def_partition,def_tbname_orc "
				+ " from dataload_tactics_info t where enabled = '1' and t.tactics_id='" + tactics_id;
		if ("1".equals(type)) {
			sql1 += "' and not exists (select 1 from dataload_job_log t1 where t1.table_name = t.table_name and t1.etl_date= '"+etl_date+ "'  and t1.task_status is not null and t1.batch_id='"
					+ thread_id + "') order by load_priority;";
		} else if ("0".equals(type)) {
			sql1 += "' and not exists (select 1 from dataload_job_log t1 where t1.table_name = t.table_name and t1.etl_date= '"+etl_date+ "'  and t1.task_status is not null ) order by load_priority;";
		} else if ("2".equals(type)) {
			sql1 += "' and t.job_id in ('" + job_ids.trim().replace(",", "','")
					+ "') and not exists (select 1 from dataload_job_log t1 where t1.job_id = t.job_id and t1.etl_date= '"+etl_date+ "' and t1.task_status is not null and t1.batch_id='"
					+ thread_id + "') order by load_priority;";
		}else if ("3".equals(type)) {
			flg=3;
			sql1 +=  "'and not exists (select 1 from dataload_job_log t1 where t1.table_name = t.table_name and t1.etl_date= '"+etl_date+ "'and t1.task_status is not null and t1.task_status<>'error') order by load_priority;";
		}
		pubAPIs.writerLog("sql1:" + sql1);
		ResultSet rs1 = stmt1.executeQuery(sql1);
		while (rs1.next()) {
			result++;
			DataLoadBean dataLoadBean = new DataLoadBean();
			dataLoadBean.setTactics_id(tactics_id);
			String job_id = rs1.getString("job_id").trim().toLowerCase();
			dataLoadBean.setJob_id(job_id);
			String src_sys_id = rs1.getString("src_sys_id").trim().toLowerCase();
			dataLoadBean.setSrc_sys_id(src_sys_id);
			String src_table_type = rs1.getString("src_table_type").trim().toLowerCase();
			if (",oracle,".indexOf("," + src_table_type + ",") == -1) {
				pubAPIs.writerLog("tactics_info表->job_id为" + job_id + "的src_table_type字段值为" + src_table_type + "不符合预期");
				continue;
			}
			dataLoadBean.setSrc_table_type(src_table_type);
			String src_file_name = rs1.getString("src_file_name");
			src_file_name = src_file_name.replace("${etl_date}", etl_date);
			dataLoadBean.setSrc_file_name(src_file_name);
			String db_name_txt = rs1.getString("db_name_txt");
			dataLoadBean.setDb_name_txt(db_name_txt);
			String db_name_orc = rs1.getString("db_name_orc");
			dataLoadBean.setDb_name_orc(db_name_orc);
			String usr_def_partition = rs1.getString("usr_def_partition");
			dataLoadBean.setUsr_def_partition(usr_def_partition);
			String table_name = rs1.getString("table_name");
			dataLoadBean.setTable_name(table_name);
			String def_tbname_orc = "";
			def_tbname_orc=rs1.getString("def_tbname_orc");
			dataLoadBean.setTable_name_txt(("t_" + src_sys_id + "_" + table_name + "_txt").toLowerCase());
			if(def_tbname_orc == null || "".equals(def_tbname_orc.trim())) {
				dataLoadBean.setTable_name_orc(("t_" + src_sys_id + "_" + table_name + "_orc").toLowerCase());
				dataLoadBean.setTable_full_name_orc(db_name_orc + ".t_" + src_sys_id + "_" + table_name + "_orc");
				dataLoadBean.setTable_full_name_txt(db_name_txt + ".t_" + src_sys_id + "_" + table_name + "_txt");
			}else {
				dataLoadBean.setTable_name_orc((def_tbname_orc).toLowerCase());
				dataLoadBean.setTable_full_name_orc(db_name_orc + "." + def_tbname_orc );
				dataLoadBean.setTable_full_name_txt(db_name_txt + ".t_" + src_sys_id + "_" + table_name + "_txt");
			}
			

			String load_type = rs1.getString("load_type");
			if (",full,add,merge,update,".indexOf("," + load_type + ",") == -1) {
				pubAPIs.writerLog("tactics_info表->job_id为" + job_id + "的load_type字段值为" + load_type + "不符合预期");
				continue;
			}
			dataLoadBean.setLoad_type(load_type);
			String col_map_type = rs1.getString("col_map_type");
			if (",1,".indexOf("," + col_map_type + ",") == -1) {
				pubAPIs.writerLog("tactics_info表->job_id为" + job_id + "的col_map_type字段值为" + col_map_type + "不符合预期");
				continue;
			}
			dataLoadBean.setCol_map_type(col_map_type);

			String load_condition = rs1.getString("load_condition");
			if (("merge".equals(load_type) || "update".equals(load_type))
					&& ("".equals(load_condition) || load_condition == null)) {
				pubAPIs.writerLog("tactics_info表->job_id为" + job_id + "的load_condition字段值为" + load_condition + "不符合预期");
				continue;
			}
			dataLoadBean.setLoad_condition(load_condition);

			// 拼接相关的文件目录及文件名称
			String datafile_hist_dir = dataFile_hist_dir + etl_date + "/" + src_sys_id + "/" + src_file_name;
			dataLoadBean.setDataFile_hist_dir(datafile_hist_dir);
			String datafile_curr_dir = dataFile_curr_dir + etl_date + "/" + src_sys_id.toUpperCase();
			dataLoadBean.setDataFile_curr_dir(datafile_curr_dir);
			// 定义当前文件位置
			String datafile_curr_end_file = datafile_curr_dir + "/" + src_file_name + ".END";
			dataLoadBean.setDatafile_curr_end_file(datafile_curr_end_file);
			String datafile_curr_sql_file = datafile_curr_dir + "/" + src_file_name + ".SQL";
			dataLoadBean.setDatafile_curr_sql_file(datafile_curr_sql_file);
			String datafile_curr_txt_file = datafile_curr_dir + "/" + src_file_name + ".TXT";
			dataLoadBean.setDatafile_curr_txt_file(datafile_curr_txt_file);
			// 定义历史文件位置
			String datafile_hist_end_file = datafile_hist_dir + "/" + src_file_name + ".END";
			dataLoadBean.setDatafile_hist_end_file(datafile_hist_end_file);
			String datafile_hist_sql_file = datafile_hist_dir + "/" + src_file_name + ".SQL";
			dataLoadBean.setDatafile_hist_sql_file(datafile_hist_sql_file);
			String datafile_hist_txt_file = datafile_hist_dir + "/data/" + src_file_name + ".TXT";
			dataLoadBean.setDatafile_hist_txt_file(datafile_hist_txt_file);

			dataLoadBean.setThread_id(thread_id);
			dataLoadBean.setBatch_id(thread_id);
			dataLoadBean.setAuthUserTDH(authUserTDH);
			dataLoadBean.setAuthUserMysql(authUserMysql);
			dataLoadBean.setConf_hdfs(conf_hdfs);
			dataLoadBean.setLogin_kerberos(login_kerberos);
			dataLoadBean.setDelimited(delimited);
			dataLoadBean.setColumnMap(columnMap);
			dataLoadBean.setLog_file_dir(log_file_dir);
			dataLoadBean.setEtl_date(etl_date);
			dataLoadBean.setType(flg);
			dataLoadBeanList.add(dataLoadBean);
		}
		rs1.close();
		stmt1.close();
		conn_mysql.close();
		FileSystem fs = DataLoadPub.getFileSystem(conf_hdfs, login_kerberos);
		for (int i = 0; i < dataLoadBeanList.size(); i++) {
			DataLoadBean dataLoadBean = dataLoadBeanList.get(i);
			if (!DataLoadPub.checkENDFile(dataLoadBean, fs)) {
				if (!DataLoadPub.checkHistENDFile(dataLoadBean, fs)) {
					pubAPIs.writerLog("ERROR:" + dataLoadBean.getTable_name() + "表对应的END文件不存在!-->"+dataLoadBean.getDatafile_curr_end_file());
					continue;
				}
			} else {
				if (!DataLoadPub.checkSQLFile(dataLoadBean, fs)) {
					pubAPIs.writerLog("ERROR:" + dataLoadBean.getTable_name() + "表对应的SQL文件不存在!-->"+dataLoadBean.getDatafile_curr_sql_file());
					continue;
				}
				if (!DataLoadPub.checkTXTFile(dataLoadBean, fs)) {
					pubAPIs.writerLog("ERROR:" + dataLoadBean.getTable_name() + "表对应的TXT文件不存在!--->"+dataLoadBean.getDatafile_curr_txt_file());
					continue;
				}
				DataLoadPub.moveENDFile(dataLoadBean, fs);
				DataLoadPub.moveTXTFile(dataLoadBean, fs);
				DataLoadPub.moveSQLFile(dataLoadBean, fs);
			}
			SchedulerThread schedulerThread = new SchedulerThread();
			schedulerThread.setParamBean(dataLoadBean);
			if (m > 0) {
				DataLoadPub.threadLock(thread_id, "add", "", 0);
			}
			m++;
			schedulerThread.start();
			if (DataLoadPub.threadLock(thread_id, "get", "schedulerThread", 0) >= parallSpotInt) {// 还跑的作业数表到parallSpotInt个
				break;
			}
		}
		if (m == 0) {
			DataLoadPub.threadLock(thread_id, "minus", "schedulerThread", 0);
		}
		return result;
	}

	private class SchedulerThread extends Thread {
		DataLoadBean dataLoadBean = new DataLoadBean();
		private void setParamBean(DataLoadBean dataLoadBean) throws Exception {
			BeanUtils.copyProperties(this.dataLoadBean, dataLoadBean);
			dataLoadBean.setColumnMap(dataLoadBean.getColumnMap());
		}

		public void run() {
			DataLoadMain dataHandleMain = new DataLoadMain();
			for (int i = 0; i < reRun; i++) {
				int result = dataHandleMain.dataHandle(dataLoadBean);
				if (result == 0) {
					break;
				}
			}
			DataLoadPub.threadLock(thread_id, "minus", "", 0);
		}		
	}

	public static void main(String[] args) {
		DataLoadScheduler schedulerTool = new DataLoadScheduler();
		String tactics_id = "erm";
		String etl_date = "20200107";
		String type = "1";// 0正常跑批;
		String job_ids = "";
		if (args.length == 3) {
			tactics_id = args[0];
			etl_date=args[1];
			type = args[2];
		} else if (args.length == 4) {
			tactics_id = args[0];
			etl_date=args[1];
			type = args[2];
			job_ids = args[3];
		} else {

		}
		schedulerTool.setJob_ids(job_ids);
		int flg = schedulerTool.runByBatch_ids(tactics_id, etl_date, type);
		System.exit(flg);
	}
}
