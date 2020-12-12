package cn.com.dataHandle;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.io.FileUtils;


import cn.com.dataHandle.bean.MetadataOCABean;
import cn.com.dataHandle.bean.MetadataTDHBean;
import cn.com.dataHandle.bean.TranfersDataBean;
import cn.com.dataHandle.metadataHandle.MetadataOCA;
import cn.com.oracleTest.CommonShellExecutor;
import cn.com.pub.PubAPIs;
import cn.com.pub.PubException;


public class EtlDataImp {
	// 导入Data
	private PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private String lock_table = "";
	private String etl_date = "";
	private String home_path = "";
	private String tab_full_name = "";
	private String lastChar = "";
	String exp_type = "";
	private Connection conn_tdh = null;
	private Connection conn_oracle = null;
	private int out_times = -1;
	private int parall_spot = 3;
	private String src_tab_full_name = "";
	//private String table_name = "";
	EtlDataPub etlDataPub = new EtlDataPub();

	public void importData(TranfersDataBean tranfersDataBean) {
		EtlDataPub.write_etlimp_jobLog(tranfersDataBean, "0", "作业开始");
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
		//table_name = tranfersDataBean.getTab_name();
		src_tab_full_name = tranfersDataBean.getSrc_tab_full_name();

		tranfersDataBean.setStartTime(start1);
		String batch_id = UUID.randomUUID().toString().replace("-", "").replace(".", "");
		tranfersDataBean.setBatch_id(batch_id);
		if ("".equals(lock_table)) {
			lock_table = UUID.randomUUID().toString().replace("-", "").replace(".", "");
			TransferDataPub.threadLock(lock_table, "add", tab_full_name, 0);
		}
		TransferDataPub.threadCheckDataImport(tab_full_name, "add");
		pubAPIs.writerLog("->" + TransferDataPub.threadLock(lock_table, "get", tab_full_name, 0) + " times ");
		try {
			conn_oracle = EtlDataPub.createOracleConnSM4("dataImport_oracle_login_ldap");
			Statement stmt1 = conn_oracle.createStatement();
			conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
			String filePath_tab = home_path + etl_date + lastChar + src_tab_full_name + lastChar;// 对应表的一级目录
			//String trun_sql_file_path = filePath_tab + "trun.sql";
			//String drop_file_path = filePath_tab + "drop.sql";
			String create_file_path = filePath_tab + "create.sql";
			//String crt_path = filePath_tab + table_name + ".ctl";

			//String remove_field = "," + tranfersDataBean.getRemove_fields() + ",";
			//String lintxt = "";
			
			String create_sql = "";
			File create_sql_file = new File(create_file_path);
			if (create_sql_file.exists()) {
				create_sql = FileUtils.readFileToString(create_sql_file);
			}
			//create_sql+=";";
			String create_sql2=create_sql.replace("\r\n","").replace(";","");
			System.out.println("create_sql:"+create_sql2);
			boolean res = etlDataPub.checktable(tranfersDataBean.getTab_name());
            System.out.println("res:"+res);
			if (!res) {
				stmt1.executeUpdate(create_sql2);
			}
			stmt1.close();
			MetadataOCA metadataOCA = new MetadataOCA();
			MetadataOCABean metadataBean_db = metadataOCA.analysOCAMetadataFromDB(tranfersDataBean);
			MetadataOCABean metadataBean_tdh = metadataOCA.analysOCAMetadataFromTDH(tranfersDataBean);//需要修改为查询mysql的相关配置表信息
			ArrayList<String[]> tar_columns_list = metadataBean_db.getTar_columns_list();
			ArrayList<String[]> src_columns_list = metadataBean_tdh.getSrc_columns_list();
			int n1 = tar_columns_list.size();// 目标表字段个数
			int l1 = src_columns_list.size();// 源表字段个数
			System.out.println(n1+"----->"+l1);
			if (l1 < n1) {
				pubAPIs.writerLog("源表字段比目标表的字段少");
				throw new PubException("源表字段小于目标表字段");
			}
			if (l1 == n1) {
				Statement stmt2 = conn_oracle.createStatement();
				StringBuffer replaceColumn = new StringBuffer();
				int replaceFlg = 0;
				for (int i = 0; i < n1; i++) {
					String src_column = src_columns_list.get(i)[0].trim().toLowerCase().replace("`", "");
					String tar_column = tar_columns_list.get(i)[0].trim().toLowerCase().replace("`", "");
					replaceColumn.append(src_columns_list.get(i)[0].trim().replace("`", "") + " ");
					replaceColumn.append(src_columns_list.get(i)[1].trim() + ",");
					if (src_column.equals(tar_column)) {
						if (!src_columns_list.get(i)[1].trim().equals(tar_columns_list.get(i)[1].trim())) {
							if (src_columns_list.get(i)[1].toLowerCase().indexOf("varchar") != -1
									&& tar_columns_list.get(i)[1].toLowerCase().indexOf("varchar2") != -1) {
								replaceFlg = 1;
								pubAPIs.writerLog("字段类型varchar精度改变:" + tar_columns_list.get(i)[0] + "---"
										+ tar_columns_list.get(i)[1]);
							} else {
								stmt2.close();
								throw new PubException("源表" + src_column + "字段类型与目标表字段类型不一致,分別为："
										+ src_columns_list.get(i)[1] + "-----" + tar_columns_list.get(i)[1]);
							}
						}
					} else {
						stmt2.close();
						throw new PubException("源表字段名与目标表字段名不一致");
					}
				}
			}
				
				if (l1 > n1 ) {
					Statement stmt2 = conn_tdh.createStatement();
					for (int k = n1; k < l1; k++) {
						String[] addColumn = src_columns_list.get(k);
						String addstr = "ALTER TABLE " + tranfersDataBean.getTab_name() + " ADD COLUMNS("
								+ addColumn[0].replace("`", "") + " " + addColumn[1] + ");";
						stmt2.execute(addstr);
						pubAPIs.writerLog("字段增加:" + addColumn[0].replace("`", "") + "---" + addColumn[1]);
					}
					stmt2.close();
				} 
				
			String linJdbc=etlDataPub.lincoon("dataImport_oracle_login_ldap");
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
					TransferDataPub.threadCheckDataImport(tab_full_name, "add");
				}
				if (isRun) {
					
					EtlImportThread etlImportThread = new EtlImportThread(tranfersDataBean, metadataOCA,linJdbc);
					etlImportThread.start();
					m++;
					break;
				}
				Thread.sleep(1000);
			}

			if (m == 0) {// 指不需要进入到子线程内就可以处理完的时候需要减一个
				TransferDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
				TransferDataPub.threadCheckDataImport(tab_full_name, "minus");
			}

			int n = 0;
			while (TransferDataPub.threadCheckDataImport(tab_full_name, "get")[0] != 0 && m > 0) {
				n++;
				if (n > out_times && out_times != -1) {// 运行时间过长退出
					pubAPIs.writerLog(tab_full_name + "运行时间过长退出");
					break;
				}
				Thread.sleep(1000);
			}
			int result1 = TransferDataPub.threadCheckDataImport(tab_full_name, "get")[1];
			if (result1 != m) {
				String errorInfo = tab_full_name + "表落地失败!" + result1 + "|" + m;
				throw new PubException(errorInfo);
			}
			EtlDataPub.write_etlimp_jobLog(tranfersDataBean, "2", "作业结束");
			m++;
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			EtlDataPub.write_etlimp_jobLog(tranfersDataBean, "1", eStr);
			pubAPIs.writerLog(eStr);
		} finally {
			if (m == 0) {// 没有处理完就结束了
				String errorInfo = "未知错误!";
				TransferDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
				pubAPIs.writerLog(errorInfo + " 当前threadLock值为："
						+ TransferDataPub.threadLock(lock_table, "get", tab_full_name, 0));
			}
			try {
				if (conn_tdh != null && !conn_tdh.isClosed()) {
					conn_tdh.close();
				}
			} catch (SQLException e1) {
				String eStr1 = pubAPIs.getException(e1);
				pubAPIs.writerLog(eStr1);
			}
			long end1 = new Date().getTime();
			tatol1 = (int) ((end1 - start1) * 1 / 1000);
			pubAPIs.writerLog("Total time:" + tatol1 + " s.");
		}
		
	}

	private class EtlImportThread extends Thread {
		private PubAPIs pubAPIs = new PubAPIs("dataHandle");
		private Connection conn_oracle = null;
		private TranfersDataBean tranfersDataBean = new TranfersDataBean();
		private MetadataTDHBean metadataBean = new MetadataTDHBean();
		private String tab_full_name = "";
        private String table_name="";
		private String lock_table = "";
		private String linJdbc="";
        private String home_path="";
        private String etl_date="";
        private String log_path="";
		private EtlImportThread(TranfersDataBean tranfersDataBean,MetadataOCA metadataOCA , String linJdbc)
				throws Exception {
			BeanUtils.copyProperties(this.tranfersDataBean, tranfersDataBean);
			BeanUtils.copyProperties(this.metadataBean, metadataBean);
			
			this.table_name=tranfersDataBean.getTab_name();
			this.linJdbc = linJdbc;
			this.lock_table = tranfersDataBean.getLock_table();
			this.tab_full_name = tranfersDataBean.getTab_full_name();
			this.home_path=tranfersDataBean.getHome_path();
			this.etl_date=tranfersDataBean.getEtl_date();
			this.log_path=tranfersDataBean.getLog_file_dir();
		}

		public void run() {
			pubAPIs.writerLog("T->" + TransferDataPub.threadLock(lock_table, "get", tab_full_name, 0) + " times ");
			conn_oracle = EtlDataPub.createOracleConnSM4("dataImport_oracle_login_ldap");
			try {
				int re_run = tranfersDataBean.getRe_run();
				for (int i = 0; i < re_run; i++) {
					int result = run_ft();
					if (result == 0) {
						TransferDataPub.threadCheckDataImport(tab_full_name, "add2");
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
				pubAPIs.writerLog(eStr);
			} finally {
				try {
					if (conn_oracle != null && !conn_oracle.isClosed()) {
						conn_oracle.close();
					}
				} catch (SQLException e1) {
					String eStr1 = pubAPIs.getException(e1);
					pubAPIs.writerLog(eStr1);
				}
				TransferDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
				TransferDataPub.threadCheckDataImport(tab_full_name, "minus");
			}
		}

		public int run_ft() {
			int result = 0;
			InputStream ins = null;
			String file_path = home_path + etl_date + lastChar + src_tab_full_name + lastChar;// 对应表的一级目录
			String crt_path = file_path + table_name + ".ctl";
			//String crt_file=crt_path+table_name+".ctl";
			//String crt_log=log_path+table_name+".log";
			
	       // String dos="sqlldr "+linJdbc+" control="+crt_file+ "log="+crt_log;
			String dos="source /home/oracle/.bash_profile;sqlldr 'tdh/tdh@orcl' control='/home/oracle/ciyutest.ctl' log='/home/oracle/ciyu.log'";
	        //String[] cmd = {"/bin/bash","-c","echo $ORACLE_HOME;echo $LD_LIBRARY_PATH;$ORACLE_HOME/bin/"+dos };
			CommonShellExecutor commonShell = new CommonShellExecutor();
			try {
				int ret = 0;
				ret = commonShell.exec("ls");
				System.out.println(ret);
				//ret = commonShell.exec("source /home/oracle/.bash_profile;sqlldr 'tdh/tdh@orcl' control='/home/oracle/ciyutest.ctl' log='/home/oracle/ciyu.log'");
				ret=commonShell.exec(dos);
				System.out.println(ret);
				commonShell.close();
//				Process process = Runtime.getRuntime().exec(dos);
//	            ins = process.getInputStream(); // 获取执行cmd命令后的信息
//	            BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
//	            String line = null;
//	            while ((line = reader.readLine()) != null)
//	            {
//	                String msg = new String(line.getBytes("ISO-8859-1"), "UTF-8");
//	                System.out.println(msg); // 输出
//	            }
	            //int exitValue = process.waitFor();
				
	            if(ret==0)
	            {
	                System.out.println("返回值：" + ret+"\n数据导入成功");
	                
	            }else
	            {
	                System.out.println("返回值：" + ret+"\n数据导入失败");
	                
	            }
	            
	           // process.getOutputStream().close(); // 关闭
	        }   
	        catch (Exception e)
	        {    e.printStackTrace();
	        	String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog("error:" + tab_full_name + "\n" + eStr);
				return -1;
	           
	        }
	    	return result;
	    }
		
	}

}
