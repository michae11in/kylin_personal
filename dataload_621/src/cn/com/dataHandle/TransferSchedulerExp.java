package cn.com.dataHandle;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import cn.com.dataHandle.bean.TranfersDataBean;
import cn.com.pub.PubAPIs;
import cn.com.pub.PubException;

public class TransferSchedulerExp {
	private PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private String lock_table = UUID.randomUUID().toString().replace("-", "").replace(".", "");
	private Connection conn_mysql = null;
	private String mysql_login_ldap = "";
	private String tdh_login_ldap = "";
	private String home_path = "";
	private String conf_hdfs = "";
	private String login_kerberos = "";
	private String temp_database = "";
	private String log_file_dir = "";
	private int parall_spot = 5;
	private int re_run = 3;
	private String job_ids = "";

	public void setJob_ids(String job_ids) {
		this.job_ids = job_ids;
	}

	public int runByBatch_ids(String tactics_id, String etl_date, String type) {
		int retNum=0;
		try {
			String name =ManagementFactory.getRuntimeMXBean().getName();
			String pid =name.split("@")[0];
			pubAPIs.writerLog("数据加载开始:"+tactics_id+":"+type+":"+pid);
			if ("2".equals(type) && "".equals(job_ids.trim())) {
				throw new PubException("传入参数不正确:当type为2时job_ids值不能为空，必须是job_id的拼接，多个job_id间用,分开");
			}
			if (",0,1,2,3,".indexOf("," + type + ",") == -1) {
				throw new PubException("type值必须为：0，1，2");
			}
			mysql_login_ldap = "dataExport_mysql_login_ldap";
			conn_mysql = TransferDataPub.createMysqlConnSM4(mysql_login_ldap);
			Statement stmt0 = conn_mysql.createStatement();
			String sql0 = "select home_path,log_file_dir,tdh_login_ldap,conf_hdfs,login_kerberos,parall_spot,re_run,temp_database from transfer_exp_prop_info where tactics_id = '"
					+ tactics_id + "';";
			pubAPIs.writerLog("sql0:" + sql0);
			ResultSet rs0 = stmt0.executeQuery(sql0);
			while (rs0.next()) {
				home_path = rs0.getString("home_path");
				tdh_login_ldap = rs0.getString("tdh_login_ldap");
				conf_hdfs = rs0.getString("conf_hdfs");
				login_kerberos = rs0.getString("login_kerberos");
				parall_spot = rs0.getInt("parall_spot");
				re_run = rs0.getInt("re_run");
				temp_database = rs0.getString("temp_database");
				log_file_dir = rs0.getString("log_file_dir");
			}
			rs0.close();
			stmt0.close();
			conn_mysql.close();

			PubAPIs.setLogsPath(log_file_dir + "TranfersDataExp" + PubAPIs.getDirSign());
			PubAPIs.setErrorLogsPath(log_file_dir + "TranfersDataExpError" + PubAPIs.getDirSign());
            
				while (true) {
					int result = runByBatch_id(tactics_id, etl_date, type);
				//	Calendar cal = Calendar.getInstance();
				//	int hour = cal.get(Calendar.HOUR_OF_DAY);
					if (result == 0) {
						break;
					} else {
						Thread.sleep(30000);
					}
				}	
		String sql2 = "";
		if(type.equals("2")) {
			sql2 = "select table_name,job_id from transfer_exp_job_log where etl_date = '"+etl_date+ "' and tactics_id = '"+tactics_id+"' and task_status = 'running' "
					+" and job_id in ('" + job_ids.trim().replace(",", "','")+"') ;";
			}else {
			 sql2 = "select table_name,job_id from transfer_exp_job_log where etl_date = '"+etl_date+ "' and tactics_id = '"+tactics_id+"' and task_status = 'running';";
			}			
		
		pubAPIs.writerLog("if_running:"+sql2);
		boolean flg =true;
		ResultSet rs2 =null;
		while(flg) {
			String  strTmp=""; 
			Connection conn_mysq2 = DataLoadPub.createMysqlConnSM4(mysql_login_ldap);
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
			pubAPIs.writerLog(tactics_id+"：等待"+strTmp+"表落地数据！");
			Thread.sleep(30000);
		}
		if(type.equals("3")) {
			String check_sql="select job_id from transfer_exp_tactics_info t where t.enabled = '1' and t.tactics_id='" + tactics_id
					         +"' and  exists (select 1 from (select * from  (select * from transfer_exp_job_log order by str_to_date(end_time,'%Y-%m-%d %H:%i:%s') desc ) a group by table_name) t1 where t1.table_name = t.tab_name and t1.etl_date= '"+etl_date+ "'and t1.task_status ='error1');";
			Connection conn_mysq3 = DataLoadPub.createMysqlConnSM4(mysql_login_ldap);
			Statement stmt3 = conn_mysq3.createStatement();
			ResultSet rs3 = stmt3.executeQuery(check_sql);
			while (rs3.next()) {
				retNum=1;
			}
			rs3.close();
			stmt3.close();
			conn_mysq3.close();
		}
		pubAPIs.writerLog("数据加载结束:"+tactics_id+":"+type+":"+pid);			
		} catch (Exception e) {
			//retNum=3;
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
		int flg =-1;
		int result = 0;
		int m = 0;
		if (TransferDataPub.threadLock(lock_table, "check", "schedulerThread", parall_spot) != -1) {// 如果上一轮所有作业都没有跑完
			return -1;
		}
		conn_mysql = TransferDataPub.createMysqlConnSM4(mysql_login_ldap);
		// 从配置表读取每个表的配置信息
		ArrayList<TranfersDataBean> tranfersDataBeanList = new ArrayList<TranfersDataBean>();
		Statement stmt1 = conn_mysql.createStatement();
		String sql1 = "select job_id,db_name,tab_name,exec_type,exec_sql from transfer_exp_tactics_info t where enabled = '1' and tactics_id = '"
				+ tactics_id;
		if ("1".equals(type)) {
			sql1 += "' and not exists (select 1 from transfer_exp_job_log t1 where t1.job_id = t.job_id and t1.etl_date= '"+etl_date+ "'  and t1.task_status is not null and t1.batch_id='"
					+ lock_table + "') order by exec_priority;";
		} else if ("0".equals(type)) {
			sql1 += "' and not exists (select 1 from transfer_exp_job_log t1 where t1.table_name = t.tab_name and t1.etl_date= '"+etl_date+ "'  and t1.task_status is not null ) order by exec_priority;";
		} else if ("2".equals(type)) {
			sql1 += "' and t.job_id in ('" + job_ids.trim().replace(",", "','")
					+ "') and not exists (select 1 from transfer_exp_job_log t1 where t1.job_id = t.job_id and t1.etl_date= '"+etl_date+ "'  and  t1.task_status is not null and t1.batch_id='"
					+ lock_table + "') order by exec_priority;";
		}else if ("3".equals(type)) {
			flg=3;
			sql1 += "' and not exists (select 1 from (select * from  (select * from transfer_exp_job_log where etl_date= '"+etl_date+ "' order by str_to_date(ifnull(end_time,sysdate()),'%Y-%m-%d %H:%i:%s') desc ) a group by table_name)  t1 where t1.table_name = t.tab_name and t1.etl_date= '"+etl_date+"' and t1.task_status is not null and t1.task_status<>'error') order by exec_priority;";
		}
		pubAPIs.writerLog("sql1:" + sql1);
		ResultSet rs1 = stmt1.executeQuery(sql1);
		while (rs1.next()) {
			result++;
			TranfersDataBean tranfersDataBean = new TranfersDataBean();
			tranfersDataBean.setTactics_id(tactics_id);
			tranfersDataBean.setEtl_date(etl_date);
			String job_id = rs1.getString("job_id").trim().toLowerCase();
			tranfersDataBean.setJob_id(job_id);
			String db_name = rs1.getString("db_name").trim().toLowerCase();
			tranfersDataBean.setDb_name(db_name);
			String tab_name = rs1.getString("tab_name").trim().toLowerCase();
			tranfersDataBean.setTab_name(tab_name);
			tranfersDataBean.setTab_full_name(db_name + "." + tab_name);
			String exec_type = rs1.getString("exec_type").trim().toLowerCase();
			tranfersDataBean.setExec_type(exec_type);
			String exec_sql = rs1.getString("exec_sql").trim().toLowerCase();
			tranfersDataBean.setExec_sql(exec_sql);

			tranfersDataBean.setConf_hdfs(conf_hdfs);
			tranfersDataBean.setHome_path(home_path);
			tranfersDataBean.setMysql_login_ldap(mysql_login_ldap);
			tranfersDataBean.setLogin_kerberos(login_kerberos);
			tranfersDataBean.setParall_spot(parall_spot);
			tranfersDataBean.setRe_run(re_run);
			tranfersDataBean.setTdh_login_ldap(tdh_login_ldap);
			tranfersDataBean.setTemp_database(temp_database);
			tranfersDataBean.setLog_file_dir(log_file_dir);
			tranfersDataBean.setType(flg);
			tranfersDataBeanList.add(tranfersDataBean);
		}
		rs1.close();
		stmt1.close();
		conn_mysql.close();
		for (int i = 0; i < tranfersDataBeanList.size(); i++) {
			TranfersDataBean tranfersDataBean = tranfersDataBeanList.get(i);
			tranfersDataBean.setLock_table(lock_table);
			tranfersDataBean.setBatch_id(lock_table);
			SchedulerThread schedulerThread = new SchedulerThread(tranfersDataBean);
			if (m > 0) {
				TransferDataPub.threadLock(lock_table, "add", "", 0);
			}
			m++;
			schedulerThread.start();
			if (TransferDataPub.threadLock(lock_table, "get", "schedulerThread", 0) >= parall_spot) {// 还跑的作业数表到parallSpotInt个
				break;
			}
		}
		if (m == 0) {
			TransferDataPub.threadLock(lock_table, "minus", "schedulerThread", 0);
		}
		return result;
	}

	private class SchedulerThread extends Thread {
		private TranfersDataBean tranfersDataBean = new TranfersDataBean();

		public SchedulerThread(TranfersDataBean tranfersDataBean) throws Exception {
			BeanUtils.copyProperties(this.tranfersDataBean, tranfersDataBean);
		}

		public void run() {
			try {
				TranfersDataExp dataExport = new TranfersDataExp();
				dataExport.exportData(tranfersDataBean);
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
			} finally {

			}
		}
	}

	public static void main(String[] args) {
		String tactics_id = "rmi";
		String etl_date = "20200710";
		String type = "0";// 0正常跑批，1强制跑批,2跑指定表，3跑批結束檢查;
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
		TransferSchedulerExp transferSchedulerExp = new TransferSchedulerExp();
		transferSchedulerExp.setJob_ids(job_ids);
		int flg = transferSchedulerExp.runByBatch_ids(tactics_id, etl_date, type);
		System.exit(flg);
	}
}
