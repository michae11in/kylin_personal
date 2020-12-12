package cn.com.dataHandle;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.fs.FileSystem;


import cn.com.dataHandle.bean.MetadataOCABean;
import cn.com.dataHandle.bean.MetadataTDHBean;
import cn.com.dataHandle.bean.TranfersDataBean;
import cn.com.dataHandle.metadataHandle.MetadataOCA;
import cn.com.pub.PubAPIs;
import cn.com.pub.PubException;
import cn.com.transferFile.EtlFiles;
import cn.com.transferFile.TransferFiles;

public class EtlDataExp {
	
	private PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private String lock_table = "";
	private FileSystem fs = null;
	private String etl_date = "";
	private String home_path = "";
	private String tab_full_name = "";
	private String lastChar = "";
	private String exp_type = "";
	private Connection conn_tdh = null;
	private int out_times = -1;
	private int parall_spot = 3;
	private String log_file_dir = "";
	private String mysql_login_ldap = "";
	private Connection conn_mysql = null;
	private String table_name = null;
	EtlDataPub etlDataPub = new EtlDataPub();

	
	public void exportData(TranfersDataBean tranfersDataBean) {
		int m = 0;
		long start1 = new Date().getTime();
		long tatol1 = 0;
		lastChar = EtlDataPub.getDirSign();
		lock_table = tranfersDataBean.getLock_table();
		home_path = tranfersDataBean.getHome_path();
		tab_full_name = tranfersDataBean.getTab_full_name();
		out_times = tranfersDataBean.getOut_times();
		parall_spot = tranfersDataBean.getParall_spot();
		exp_type = tranfersDataBean.getExec_type();
		etl_date = tranfersDataBean.getEtl_date();
		log_file_dir = tranfersDataBean.getLog_file_dir();
		table_name = tranfersDataBean.getTab_name();
		try {
			mysql_login_ldap = "dataExport_mysql_login_ldap";
			conn_mysql = DataLoadPub.createMysqlConnSM4(mysql_login_ldap);
			Statement stmt0 = conn_mysql.createStatement();
			String sql_ifRun = "select 1 from etl_exp_job_log  where table_name = '" + table_name
					+ "' and etl_date= '" + etl_date + "' and task_status='running';";
			ResultSet rs_ifRun = stmt0.executeQuery(sql_ifRun);
			if (rs_ifRun.next()) {
				pubAPIs.writerLog("其他批次正在操作" + table_name + " 表，跳出本次落地数据文件！");
				return;
			}

			EtlDataPub.write_etlexp_jobLog(tranfersDataBean, "0", "作业开始");
			PubAPIs.setLogsPath(log_file_dir + "EtlfersDataExp" + PubAPIs.getDirSign());
			PubAPIs.setErrorLogsPath(log_file_dir + "EtlfersDataExpError" + PubAPIs.getDirSign());
			pubAPIs.setClassName(tab_full_name);
			String batch_id = UUID.randomUUID().toString().replace("-", "").replace(".", "");
			tranfersDataBean.setBatch_id(batch_id);
			tranfersDataBean.setStartTime(start1);
			if ("".equals(lock_table)) {
				lock_table = UUID.randomUUID().toString().replace("-", "").replace(".", "");
				EtlDataPub.threadLock(lock_table, "add", tab_full_name, 0);
			}
			EtlDataPub.threadCheckDataExport(tab_full_name, "add");
			pubAPIs.writerLog("->" + EtlDataPub.threadLock(lock_table, "get", tab_full_name, 0) + " times ");

			conn_tdh = EtlDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
			fs = PubAPIs.getFileSystem(tranfersDataBean.getConf_hdfs(), tranfersDataBean.getLogin_kerberos(),
					"dataHandle", "get", null);
			MetadataOCA MetadataOCA = new MetadataOCA();
			MetadataOCABean metadataBean = MetadataOCA.analysOCAMetadataFromTDH(tranfersDataBean);

//			String createTabSQL = metadataBean.getCreateTableSQL();
//			String dropTabSQL = metadataBean.getDropTabSQL();
//			String truncateTabSQL=metadataBean.getTruncateTabSQL();

			String filePath_tab = home_path + etl_date + lastChar + tab_full_name + lastChar;// 对应表的一级目录
			etlDataPub.delete(new File(filePath_tab));
			new File(filePath_tab).mkdirs();
			String ok_file_path = home_path + etl_date + lastChar + tab_full_name + ".end";
			etlDataPub.delete(new File(ok_file_path));

			String trun_sql_file_path = filePath_tab + "trun.sql";
			EtlDataPub.addStringToFile(trun_sql_file_path, metadataBean.getTruncateTabSQL(), "\n");
			String data_file_path = filePath_tab + "data" + lastChar;
			tranfersDataBean.setData_file_path(data_file_path);
		

			String drop_file_path = filePath_tab + "drop.sql";
			EtlDataPub.addStringToFile(drop_file_path, metadataBean.getDropTabSQL(), "\n");
			String create_file_path = filePath_tab + "create.sql";
			EtlDataPub.addStringToFile(create_file_path, metadataBean.getCreateTableSQL(), "\n");
			//String crt_path=filePath_tab+table_name+".ctl";
			//EtlDataPub.addStringToFile(crt_path,metadataBean.getCrtlSQL(),"\n");
			int j = 0;
			while (true) {
				j++;
				if (j > out_times && out_times != -1) {
					pubAPIs.writerLog("同时有" + parall_spot + "个作业运行超过:" + j + "s");
					break;
				}
				boolean isRun = false;
				if (m == 0) {
					isRun = true;
				} else if (TransferDataPub.threadLock(lock_table, "check", tab_full_name,
						parall_spot) == -1) {
					isRun = true;
					TransferDataPub.threadCheckDataExport(tab_full_name, "add");
				}
				if (isRun) {
					TranfersDataThread tranfersDataThread = new TranfersDataThread(
							tranfersDataBean, metadataBean);
					tranfersDataThread.start();
					m++;
					break;
				}
				Thread.sleep(1000);
			}
			

			
			if (m == 0) {// 指不需要进入到子线程内就可以处理完的时候需要减一个
				EtlDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
				EtlDataPub.threadCheckDataExport(tab_full_name, "minus");
			}

			int n = 0;
			while (EtlDataPub.threadCheckDataExport(tab_full_name, "get")[0] != 0 && m > 0) {
				n++;
				if (n > out_times && out_times != -1) {// 运行时间过长退出
					pubAPIs.writerLog(tab_full_name + "运行时间过长退出");
					break;
				}
				Thread.sleep(1000);
			}
			
			int result1 = EtlDataPub.threadCheckDataExport(tab_full_name, "get")[1];
			if (result1 != m) {
				String errorInfo = tab_full_name + "表落地失败!" + result1 + "|" + m;
				throw new PubException(errorInfo);
			}
			EtlDataPub.write_etlexp_jobLog(tranfersDataBean, "2", "作业结束");
			EtlDataPub.addStringToFile(ok_file_path, "success", "");
			m++;
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			EtlDataPub.write_etlexp_jobLog(tranfersDataBean, "1", eStr);
			pubAPIs.writerErrorLog(eStr);
		} finally {
			if (m == 0) {// 没有处理完就结束了
				String errorInfo = "未知错误!";
				EtlDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
				pubAPIs.writerLog(errorInfo + " 当前threadLock值为："
						+ EtlDataPub.threadLock(lock_table, "get", tab_full_name, 0));
			}
			try {
				PubAPIs.getFileSystem("", "", "dataHandle", "close", fs);
				if (conn_tdh != null && !conn_tdh.isClosed()) {
					conn_tdh.close();
				}
			} catch (Exception e1) {
				String eStr1 = pubAPIs.getException(e1);
				pubAPIs.writerErrorLog(eStr1);
			}
			long end1 = new Date().getTime();
			tatol1 = (int) ((end1 - start1) * 1 / 1000);
			pubAPIs.writerLog("Total time:" + tatol1 + "s.");
		}
	}

	private class TranfersDataThread extends Thread {
		private PubAPIs pubAPIs = new PubAPIs("dataHandle");
		private Connection conn_tdh = null;
		private TranfersDataBean tranfersDataBean = new TranfersDataBean();
		private MetadataTDHBean metadataBean = new MetadataTDHBean();
		private String log_file_dir = "";

		private TranfersDataThread(TranfersDataBean tranfersDataBean, MetadataOCABean metadataBean) throws Exception {
			BeanUtils.copyProperties(this.tranfersDataBean, tranfersDataBean);
			BeanUtils.copyProperties(this.metadataBean, metadataBean);
			this.log_file_dir = tranfersDataBean.getLog_file_dir();
			PubAPIs.setLogsPath(log_file_dir + "TranfersDataExp" + PubAPIs.getDirSign());
			PubAPIs.setErrorLogsPath(log_file_dir + "TranfersDataExpError" + PubAPIs.getDirSign());
			this.pubAPIs.setClassName(tab_full_name);
		}

		public void run() {
			pubAPIs.writerLog("T->" + EtlDataPub.threadLock(lock_table, "get", tab_full_name, 0) + " times ");
			conn_tdh = EtlDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
			try {
				int re_run = tranfersDataBean.getRe_run();
				for (int i = 0; i < re_run; i++) {
					int result = run_ft();
					if (result == 0) {
						EtlDataPub.threadCheckDataExport(tab_full_name, "add2");
						break;
					} else {
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							String eStr = pubAPIs.getException(e);
							pubAPIs.writerLog("Thread.sleep:" + eStr);
						}
					}
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerErrorLog(eStr);
			} finally {
				try {
					if (conn_tdh != null && !conn_tdh.isClosed()) {
						conn_tdh.close();
					}
				} catch (SQLException e1) {
					String eStr1 = pubAPIs.getException(e1);
					pubAPIs.writerErrorLog(eStr1);
				}
				EtlDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
				EtlDataPub.threadCheckDataExport(tab_full_name, "minus");
			}
		}

		public int run_ft() {
			int result = 0;
			try {
				
				String local_file_path= tranfersDataBean.getData_file_path();
				System.out.println("local_file_path"+local_file_path);
				String filePath_tab = home_path + etl_date + lastChar + tab_full_name + lastChar;// 对应表的一级目录
				//String data_trget_path ="/export/crm/"+etl_date+"/"+table_name+"/";
				String data_trget_path="/export/crm/"+table_name+"/";
				String exp_sql="";
				if("all".equals(exp_type)) {
					exp_sql="insert overwrite  directory '" + data_trget_path +"'  select concat_ws('#&',array(t.*)) from " + tab_full_name +" t;";

				}else if("auto".equals(exp_type)) {
					exp_sql="insert overwrite  directory '" + data_trget_path +"' select concat_ws('#&',array(t.*)) from " + tab_full_name +" t where etl_date = '"+ etl_date +"';";
				}else {
					exp_sql="insert overwrite  directory '" + data_trget_path +"' select concat_ws('#&',array(t.*)) from" + tab_full_name +" t where etl_date = '"+ etl_date +"';";
				}
				conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
				Statement stmt1 = conn_tdh.createStatement();
				pubAPIs.writerLog("Exp_Sql:"+exp_sql);
				stmt1.addBatch(exp_sql);
				stmt1.executeBatch();
				stmt1.close();
				
				EtlFiles etlFiles = new EtlFiles("dataHandle");
				etlFiles.transferCatalogFromTDH("dataExport_login_kerberos", "dataExport_conf_tdh",
						data_trget_path,  local_file_path + lastChar);
				MetadataOCA MetadataOCA = new MetadataOCA();
				MetadataOCABean ctldataBean = MetadataOCA.analysOCACTLFromTDH(tranfersDataBean);
				String crt_path=filePath_tab+table_name+".ctl";
				EtlDataPub.addStringToFile(crt_path,ctldataBean.getCrtlSQL(),"\n");	
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerErrorLog(tab_full_name + "\n" + eStr);
				return -1;
			}
			return result;
		}
	}
}
