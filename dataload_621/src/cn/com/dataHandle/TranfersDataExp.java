package cn.com.dataHandle;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.fs.FileSystem;

import cn.com.dataHandle.bean.MetadataTDHBean;
import cn.com.dataHandle.bean.TranfersDataBean;
import cn.com.dataHandle.metadataHandle.MetadataTDH;
import cn.com.pub.PubAPIs;
import cn.com.pub.PubException;
import cn.com.transferFile.TransferFiles;

public class TranfersDataExp {
	// 导出Data
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
	TransferDataPub transferDataPub = new TransferDataPub();

	public void exportData(TranfersDataBean tranfersDataBean) {
		int m = 0;
		long start1 = new Date().getTime();
		long tatol1 = 0;
		lastChar = TransferDataPub.getDirSign();
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
			String sql_ifRun = "select 1 from transfer_exp_job_log  where table_name = '" + table_name
					+ "' and etl_date= '" + etl_date + "' and task_status='running';";
			ResultSet rs_ifRun = stmt0.executeQuery(sql_ifRun);
			if (rs_ifRun.next()) {
				pubAPIs.writerLog("其他批次正在操作" + table_name + " 表，跳出本次落地数据文件！");
				return;
			}

			TransferDataPub.write_exp_jobLog(tranfersDataBean, "0", "作业开始");
			PubAPIs.setLogsPath(log_file_dir + "TranfersDataExp" + PubAPIs.getDirSign());
			PubAPIs.setErrorLogsPath(log_file_dir + "TranfersDataExpError" + PubAPIs.getDirSign());
			pubAPIs.setClassName(tab_full_name);
			String batch_id = UUID.randomUUID().toString().replace("-", "").replace(".", "");
			tranfersDataBean.setBatch_id(batch_id);
			tranfersDataBean.setStartTime(start1);
			if ("".equals(lock_table)) {
				lock_table = UUID.randomUUID().toString().replace("-", "").replace(".", "");
				TransferDataPub.threadLock(lock_table, "add", tab_full_name, 0);
			}
			TransferDataPub.threadCheckDataExport(tab_full_name, "add");
			pubAPIs.writerLog("->" + TransferDataPub.threadLock(lock_table, "get", tab_full_name, 0) + " times ");

			conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
			fs = PubAPIs.getFileSystem(tranfersDataBean.getConf_hdfs(), tranfersDataBean.getLogin_kerberos(),
					"dataHandle", "get", null);
			MetadataTDH metadataTDH = new MetadataTDH();
			MetadataTDHBean metadataBean = metadataTDH.analysTdhMetadataFromDB(tranfersDataBean, "all");
			String isPartition = metadataBean.getIsPartition();// false/ture
			String isRangePartition = metadataBean.getIsRangePartition();
			String inputFormat = metadataBean.getInputFormat();// orc/no_orc
			String tab_location = metadataBean.getTab_location();
			String tmp_tab_full_name = metadataBean.getTmp_tab_full_name();
			String createTmpTabSQL = metadataBean.getCreateTmpTabSQL();
			String dropTmpTabSQL = metadataBean.getDropTmpTabSQL();
			String partition_insert_head = metadataBean.getPartition_insert_head();

			List<String[]> partition_list = metadataBean.getPartition_list();
			HashMap<String, String> partition_addPartition = metadataBean.getPartition_addPartition();
			HashMap<String, String> partition_whereStr = metadataBean.getPartition_whereStr();
			HashMap<String, String> partition_trunPartition = metadataBean.getPartition_trunPartition();

			String filePath_tab = home_path + etl_date + lastChar + tab_full_name + lastChar;// 对应表的一级目录
			transferDataPub.delete(new File(filePath_tab));
			new File(filePath_tab).mkdirs();
			String ok_file_path = home_path + etl_date + lastChar + tab_full_name + ".end";
			transferDataPub.delete(new File(ok_file_path));

			String trun_sql_file_path = filePath_tab + "trun.sql";
			String add_sql_file_path = filePath_tab + "add.sql";
			String min_sql_file_path = filePath_tab + "min.sql";
			String data_file_path = filePath_tab + "data" + lastChar;
			tranfersDataBean.setData_file_path(data_file_path);
			tranfersDataBean.setMin_file_path(min_sql_file_path);

			String drop_file_path = filePath_tab + "drop.sql";
			TransferDataPub.addStringToFile(drop_file_path, metadataBean.getDropTmpTabSQL(), "\n");
			String tabInfo_file_path = filePath_tab + "tabInfo.sql";
			TransferDataPub.addStringToFile(tabInfo_file_path, metadataBean.getCreateTableSQL(), "\n");
			String tmpTabInfo_file_path = filePath_tab + "tmpTabInfo.sql";
			TransferDataPub.addStringToFile(tmpTabInfo_file_path, metadataBean.getCreateTmpTabSQL(), "\n");

			if ("auto".equals(exp_type) || "all".equals(exp_type)) {
				if ("orc".equals(inputFormat)) {
					if ("false".equals(isPartition)) {// 非事务表、非分区表
						int result = TransferDataPub.checkHisData(fs, tab_location, etl_date);// -1数据有变地;-2不存在表示被DROP了/被truncate了;0没有变动
						if (result == -2) {
							String trun_sql = "truncate table " + tab_full_name + ";";
							TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");
						} else if (result == -1 || "all".equals(exp_type)) {
							String trun_sql = "truncate table " + tab_full_name + ";";
							TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");
							TransferFiles transferFiles = new TransferFiles("dataHandle");
							transferFiles.setOut_times(out_times);
							transferFiles.setPubAPIs_className(tab_full_name);
							transferFiles.transferCatalogFromTDH("dataExport_login_kerberos", "dataExport_conf_tdh",
									tab_location, data_file_path);
							String min_sql = "all\001" + " from " + tmp_tab_full_name;
							TransferDataPub.addStringToFile(min_sql_file_path, min_sql, "\n");
						}
					} else {// 非事表、分区表
						int result = TransferDataPub.checkHisData(fs, tab_location, etl_date);// -1数据有变地;-2不存在表示被DROP了/被truncate了;0没有变动
						if (result == -2) {
							String trun_sql = "truncate table " + tab_full_name + ";";
							TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");
						} else if (result == -1 || "all".equals(exp_type)) {// 分区表有变动，全分区扫描
							for (int i = 0; i < partition_list.size(); i++) {
								String[] partition_info = partition_list.get(i);
								String partition_name = partition_info[0];
								String addPartition_str = partition_addPartition.get(partition_name);
								String trun_sql = partition_trunPartition.get(partition_name);
								result = TransferDataPub.checkHisData(fs, tab_location + partition_name, etl_date);
								if (result == -2) {// 分区表有变动某分区被清空的情况
									TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");
									TransferDataPub.addStringToFile(add_sql_file_path, addPartition_str, "\n");
								} else if (result == -1 || "all".equals(exp_type)) {
									if ("false".equals(isRangePartition)) {
										String addPartition_str_tmp = addPartition_str.replace(tab_full_name,
												tmp_tab_full_name);
										TransferDataPub.addStringToFile(tmpTabInfo_file_path, addPartition_str_tmp,
												"\n");
									}
									String min_sql = partition_name + "\001" + " from " + tmp_tab_full_name + " where "
											+ partition_whereStr.get(partition_name);
									TransferDataPub.addStringToFile(min_sql_file_path, min_sql, "\n");
									TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");
									TransferDataPub.addStringToFile(add_sql_file_path, addPartition_str, "\n");
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
													tranfersDataBean, metadataBean, partition_name);
											tranfersDataThread.start();
											m++;
											break;
										}
										Thread.sleep(1000);
									}
								}
							}
						}
					}
				} else {
					if ("false".equals(isPartition)) {
						int result = TransferDataPub.checkHisData(fs, tab_location, etl_date);// -1数据有变地;-2不存在表示被DROP了/被truncate了;0没有变动
						 if (result == -1 || "all".equals(exp_type)) {
							String trun_sql = "truncate table " + tab_full_name + ";";
							TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n"); // 改

							Statement stmt1 = conn_tdh.createStatement();
							stmt1.execute(createTmpTabSQL);
							stmt1.close();
							String tmp_tab_location = "";
							String sql2 = "SELECT t.table_location||'/' as table_location FROM system.tables_v t WHERE t.database_name='"
									+ metadataBean.getTemp_db_name() + "' AND table_name = '"
									+ metadataBean.getTemp_tab_name() + "';";
							Statement stmt2 = conn_tdh.createStatement();
							ResultSet rs2 = stmt2.executeQuery(sql2);
							while (rs2.next()) {
								tmp_tab_location = rs2.getString("table_location");
							}
							rs2.close();
							stmt2.close();

							Statement stmt3 = conn_tdh.createStatement();
							String sql3 = "insert into " + metadataBean.getTmp_tab_full_name() + " select * from "
									+ metadataBean.getTab_full_name();
							stmt3.execute(sql3);
							stmt3.close();

							TransferFiles transferFiles = new TransferFiles("dataHandle");
							transferFiles.setOut_times(out_times);
							transferFiles.setPubAPIs_className(tab_full_name);
							transferFiles.transferCatalogFromTDH("dataExport_login_kerberos", "dataExport_conf_tdh",
									tmp_tab_location + lastChar, data_file_path);

							String min_sql = "all\001" + " from " + tmp_tab_full_name;
							TransferDataPub.addStringToFile(min_sql_file_path, min_sql, "\n");
						}
						 else if (result == -2) {
								String trun_sql = "truncate table " + tab_full_name + ";";
								TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");
							} 
					} else {// 分区表，非orc表
						int result = TransferDataPub.checkHisData(fs, tab_location, etl_date);// -1数据有变地;-2不存在表示被DROP了/被truncate了;0没有变动
						if (result == -1 || "all".equals(exp_type)) {
							Statement stmt1 = conn_tdh.createStatement();
							pubAPIs.writerLog("createTmpTabSQL:" + createTmpTabSQL);
							stmt1.execute(createTmpTabSQL);
							stmt1.close();
							String tmp_tab_location = "";
							String sql2 = "SELECT t.table_location||'/' as table_location FROM system.tables_v t WHERE t.database_name='"
									+ metadataBean.getTemp_db_name() + "' AND table_name = '"
									+ metadataBean.getTemp_tab_name() + "';";
							Statement stmt2 = conn_tdh.createStatement();
							pubAPIs.writerLog("sql2:" + sql2);
							ResultSet rs2 = stmt2.executeQuery(sql2);
							while (rs2.next()) {
								tmp_tab_location = rs2.getString("table_location");
							}
							rs2.close();
							stmt2.close();
							metadataBean.setTmp_tab_location(tmp_tab_location);

							// 全分区扫描
							for (int i = 0; i < partition_list.size(); i++) {
								String[] partition_info = partition_list.get(i);
								String partition_name = partition_info[0];
								// -1数据有变地;-2不存在表示被DROP了/被truncate了;0没有变动
								result = TransferDataPub.checkHisData(fs, tab_location + partition_name, etl_date);
								String trun_sql = partition_trunPartition.get(partition_name);
								if (result == -2) {// 分区表有变动某分区被清空的情况
									TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");
								} else if (result == -1 || "all".equals(exp_type)) {
									TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");
									String min_sql = partition_name + "\001" + " from " + tmp_tab_full_name + " where "
											+ partition_whereStr.get(partition_name);
									TransferDataPub.addStringToFile(min_sql_file_path, min_sql, "\n");
									TransferDataPub.addStringToFile(add_sql_file_path,
											partition_addPartition.get(partition_name), "\n");
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
													tranfersDataBean, metadataBean, partition_name);
											tranfersDataThread.start();
											m++;
											break;
										}
										Thread.sleep(1000);
									}
								}
							}
						} else if (result == -2) {
							String trun_sql = "truncate table " + tab_full_name + ";";
							TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");
						}
					}
				}
			} else if ("sql".equals(exp_type)) {
				Statement stmt1 = conn_tdh.createStatement();
				pubAPIs.writerLog("createTmpTabSQL:" + createTmpTabSQL);
				stmt1.execute(createTmpTabSQL);
				stmt1.close();

				String tmp_tab_location = "";
				String sql2 = "SELECT t.table_location||'/' as table_location FROM system.tables_v t WHERE t.database_name='"
						+ metadataBean.getTemp_db_name() + "' AND table_name = '" + metadataBean.getTemp_tab_name()
						+ "';";
				Statement stmt2 = conn_tdh.createStatement();
				pubAPIs.writerLog("sql2:" + sql2);
				ResultSet rs2 = stmt2.executeQuery(sql2);
				while (rs2.next()) {
					tmp_tab_location = rs2.getString("table_location");
				}
				rs2.close();
				stmt2.close();

				String exp_sql = tranfersDataBean.getExec_sql();
				exp_sql = exp_sql.replace("${etl_date}", etl_date);
				String trun_sql = "delete from " + tab_full_name + " where " + exp_sql + ";";
				TransferDataPub.addStringToFile(trun_sql_file_path, trun_sql, "\n");

				Statement stmt3 = conn_tdh.createStatement();
				String sql3 = "";
				if ("false".equals(isRangePartition) && "true".equals(isPartition)) {
					sql3 = "insert into " + metadataBean.getTmp_tab_full_name() + partition_insert_head
							+ " select * from " + metadataBean.getTab_full_name() + " where " + exp_sql;
				} else {
					sql3 = "insert into " + metadataBean.getTmp_tab_full_name() + " select * from "
							+ metadataBean.getTab_full_name() + " where " + exp_sql;
				}
				pubAPIs.writerLog("sql3:" + sql3);
				stmt3.execute("set ngmr.partition.automerge=true");
				stmt3.execute("set hive.exec.dynamic.partition=true");
				stmt3.execute(sql3);
				stmt3.close();

				TransferFiles transferFiles = new TransferFiles("dataHandle");
				transferFiles.setOut_times(out_times);
				transferFiles.setPubAPIs_className(tab_full_name);
				transferFiles.transferCatalogFromTDH("dataExport_login_kerberos", "dataExport_conf_tdh",
						tmp_tab_location, data_file_path);

				String min_sql = exp_sql + "\001" + " from " + tmp_tab_full_name;
				TransferDataPub.addStringToFile(min_sql_file_path, min_sql, "\n");
			}

			if (m == 0) {// 指不需要进入到子线程内就可以处理完的时候需要减一个
				TransferDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
				TransferDataPub.threadCheckDataExport(tab_full_name, "minus");
			}

			int n = 0;
			while (TransferDataPub.threadCheckDataExport(tab_full_name, "get")[0] != 0 && m > 0) {
				n++;
				if (n > out_times && out_times != -1) {// 运行时间过长退出
					pubAPIs.writerLog(tab_full_name + "运行时间过长退出");
					break;
				}
				Thread.sleep(1000);
			}
			if (!"orc".equals(inputFormat) || ("sql".equals(exp_type))) {
				Statement stmt5 = conn_tdh.createStatement();
				pubAPIs.writerLog("dropTmpTabSQL:" + dropTmpTabSQL);
				stmt5.execute(dropTmpTabSQL);
				stmt5.close();
			}

			int result1 = TransferDataPub.threadCheckDataExport(tab_full_name, "get")[1];
			if (result1 != m) {
				String errorInfo = tab_full_name + "表落地失败!" + result1 + "|" + m;
				throw new PubException(errorInfo);
			}
			TransferDataPub.write_exp_jobLog(tranfersDataBean, "2", "作业结束");
			TransferDataPub.addStringToFile(ok_file_path, "success", "");
			m++;
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			TransferDataPub.write_exp_jobLog(tranfersDataBean, "1", eStr);
			pubAPIs.writerErrorLog(eStr);
		} finally {
			if (m == 0) {// 没有处理完就结束了
				String errorInfo = "未知错误!";
				TransferDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
				pubAPIs.writerLog(errorInfo + " 当前threadLock值为："
						+ TransferDataPub.threadLock(lock_table, "get", tab_full_name, 0));
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
		private String partition_name = "";
		private String log_file_dir = "";

		private TranfersDataThread(TranfersDataBean tranfersDataBean, MetadataTDHBean metadataBean,
				String partition_name) throws Exception {
			BeanUtils.copyProperties(this.tranfersDataBean, tranfersDataBean);
			BeanUtils.copyProperties(this.metadataBean, metadataBean);
			this.metadataBean.setPartition_whereStr(metadataBean.getPartition_whereStr());
			this.partition_name = partition_name;
			this.log_file_dir = tranfersDataBean.getLog_file_dir();
			PubAPIs.setLogsPath(log_file_dir + "TranfersDataExp" + PubAPIs.getDirSign());
			PubAPIs.setErrorLogsPath(log_file_dir + "TranfersDataExpError" + PubAPIs.getDirSign());
			this.pubAPIs.setClassName(tab_full_name);
		}

		public void run() {
			pubAPIs.writerLog("T->" + TransferDataPub.threadLock(lock_table, "get", tab_full_name, 0) + " times ");
			conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
			try {
				int re_run = tranfersDataBean.getRe_run();
				for (int i = 0; i < re_run; i++) {
					int result = run_ft();
					if (result == 0) {
						TransferDataPub.threadCheckDataExport(tab_full_name, "add2");
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
				TransferDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
				TransferDataPub.threadCheckDataExport(tab_full_name, "minus");
			}
		}

		public int run_ft() {
			int result = 0;
			try {
				String data_file_path = tranfersDataBean.getData_file_path();
				if ("orc".equals(metadataBean.getInputFormat())) {
					String tab_location = metadataBean.getTab_location();
					TransferFiles transferFiles = new TransferFiles("dataHandle");
					transferFiles.setOut_times(out_times);
					transferFiles.setPubAPIs_className(tab_full_name);
					transferFiles.transferCatalogFromTDH("dataExport_login_kerberos", "dataExport_conf_tdh",
							tab_location + partition_name + lastChar, data_file_path + partition_name + lastChar);
				} else {
					Statement stmt1 = conn_tdh.createStatement();
					String insertTmpTab = "";
					if ("false".equals(metadataBean.getIsRangePartition())
							&& "true".equals(metadataBean.getIsPartition())) {
						insertTmpTab = "insert into " + metadataBean.getTmp_tab_full_name()
								+ metadataBean.getPartition_insert_head() + " select * from "
								+ metadataBean.getTab_full_name() + " where "
								+ metadataBean.getPartition_whereStr().get(partition_name);
					} else {
						insertTmpTab = "insert into " + metadataBean.getTmp_tab_full_name() + " select * from "
								+ metadataBean.getTab_full_name() + " where "
								+ metadataBean.getPartition_whereStr().get(partition_name);
					}
					pubAPIs.writerLog("insertTmpTab:" + insertTmpTab);
					stmt1.execute("set ngmr.partition.automerge=true");
					stmt1.execute("set hive.exec.dynamic.partition=true");
					stmt1.execute(insertTmpTab);
					stmt1.close();

					String tmp_tab_location = metadataBean.getTmp_tab_location();
					TransferFiles transferFiles = new TransferFiles("dataHandle");
					transferFiles.setOut_times(out_times);
					transferFiles.setPubAPIs_className(tab_full_name);
					transferFiles.transferCatalogFromTDH("dataExport_login_kerberos", "dataExport_conf_tdh",
							tmp_tab_location + partition_name + lastChar, data_file_path + partition_name + lastChar);
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerErrorLog(tab_full_name + "\n" + eStr);
				return -1;
			}
			return result;
		}
	}
}