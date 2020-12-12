package cn.com.dataHandle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.io.FileUtils;

import cn.com.dataHandle.bean.MetadataTDHBean;
import cn.com.dataHandle.bean.TranfersDataBean;
import cn.com.dataHandle.metadataHandle.MetadataTDH;
import cn.com.pub.PubAPIs;
import cn.com.pub.PubException;
import cn.com.transferFile.TransferFiles;

public class TranfersDataImp {
	// 导出Data
	private PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private String lock_table = "";
	private String etl_date = "";
	private String home_path = "";
	private String tab_full_name = "";
	private String lastChar = "";
	String exp_type = "";
	private Connection conn_tdh = null;
	private int out_times = -1;
	private int parall_spot = 3;
	private String temp_database = "";
	private String temp_tab_name = "";
	private String src_tab_full_name = "";
	private String tmp_tab_full_name = "";

	TransferDataPub transferDataPub = new TransferDataPub();

	public void importData(TranfersDataBean tranfersDataBean) {
		TransferDataPub.write_imp_jobLog(tranfersDataBean, "0", "作业开始");
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
		temp_database = tranfersDataBean.getTemp_database();
		src_tab_full_name = tranfersDataBean.getSrc_tab_full_name();
		temp_tab_name = src_tab_full_name.replace(".", "_");
		tmp_tab_full_name = temp_database + "." + temp_tab_name;

		tranfersDataBean.setTmp_tab_full_name(tmp_tab_full_name);
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
			conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
			String filePath_tab = home_path + etl_date + lastChar + src_tab_full_name + lastChar;// 对应表的一级目录
			String trun_sql_file_path = filePath_tab + "trun.sql";
			String add_sql_file_path = filePath_tab + "add.sql";
			String min_sql_file_path = filePath_tab + "min.sql";
			String data_file_path = filePath_tab + "data" + lastChar;
			String drop_file_path = filePath_tab + "drop.sql";
			String tabInfo_file_path = filePath_tab + "tabInfo.sql";
			String tmpTabInfo_file_path = filePath_tab + "tmpTabInfo.sql";
			String remove_field=","+tranfersDataBean.getRemove_fields()+",";
			String alltxtField="";
			String insertField="";
			StringBuffer alltxtFields=new StringBuffer();
			tranfersDataBean.setTabInfo_file_path(tabInfo_file_path);
			MetadataTDH metadataTDH = new MetadataTDH();
			MetadataTDHBean metadataBean = metadataTDH.analysTdhMetadataFromDB(tranfersDataBean, "all"); //解析目标表的表结构数据
		if (metadataBean.getResult() == -1) {
				String tabInfo_infor = "";
				File tabInfo_file = new File(tabInfo_file_path);
				if (tabInfo_file.exists()) {
					tabInfo_infor = FileUtils.readFileToString(tabInfo_file);
					if (!"".equals(tabInfo_infor)&&!tranfersDataBean.getRemove_fields().equals("")&&tranfersDataBean.getRemove_fields()!=null) {
						String[] strs = tabInfo_infor.split("\n");
						int tmp_flg1=0;
						StringBuilder tabInfo_infor_tmp = new StringBuilder();
						for (String str : strs) {
							str = str.replaceAll("\\s+", " ").trim();
							if(str.indexOf(")")!=-1)tmp_flg1=0;
							if(tmp_flg1==1){
								String field =","+str.trim().split(" ")[0]+",";
								if(remove_field.indexOf(field)!=-1) continue;
								alltxtFields.append(str.trim().split(" ")[0]+",");
							}
							if(str.indexOf("CREATE TABLE")!=-1)tmp_flg1=1;
							tabInfo_infor_tmp.append(str+"\n");
						}
						alltxtField=alltxtFields.toString();
						alltxtField=alltxtField.substring(0,alltxtField.length()-1);
						tranfersDataBean.setAlltxtField(alltxtField);
						int indexOf = tabInfo_infor_tmp.indexOf("\n)");
						 String substring = tabInfo_infor_tmp.substring(indexOf-1,indexOf);
						if(",".equals(substring)) tabInfo_infor_tmp = tabInfo_infor_tmp.replace(indexOf-1, indexOf, "");
						tabInfo_infor=tabInfo_infor_tmp.toString();
					} 
				}			
				if (!"".equals(tabInfo_infor)) {
					tabInfo_infor = tabInfo_infor.replace(src_tab_full_name, tab_full_name);
					Statement stmt1 = conn_tdh.createStatement();
					pubAPIs.writerLog("tabInfo_infor:" + tabInfo_infor);
					stmt1.addBatch(tabInfo_infor);
					stmt1.executeBatch();
					stmt1.clearBatch();
					stmt1.close();
				}
				metadataBean = metadataTDH.analysTdhMetadataFromDB(tranfersDataBean, "all");
			}
			
			
			if ("auto-partition".equals(exp_type)) {                                                                                                                                                                                                                                                              
				MetadataTDHBean metadataBean_txt = metadataTDH.analysTdhMetadataFromTXT(tranfersDataBean, "all");
				ArrayList<String[]> src_columns_list = metadataBean_txt.getSrc_columns_list();
				ArrayList<String[]> tar_columns_list = metadataBean.getTar_columns_list();
				int n1 = tar_columns_list.size();//目标表字段个数
				int l1 =src_columns_list.size();//源表字段个数
				if(l1<n1){
					pubAPIs.writerLog("源表字段比目标表的字段少");
					throw new PubException("源表字段小于目标表字段");
				}
				if(l1==n1||l1>n1){
					Statement stmt2= conn_tdh.createStatement();
					StringBuffer replaceColumn = new StringBuffer();
					int replaceFlg=0;
					for(int i=0;i<n1;i++){
						String src_column = src_columns_list.get(i)[0].trim().toLowerCase().replace("`", "");
						String tar_column = tar_columns_list.get(i)[0].trim().toLowerCase().replace("`", "");
						replaceColumn.append(src_columns_list.get(i)[0].trim().replace("`", "")+" ");
						replaceColumn.append(src_columns_list.get(i)[1].trim()+",");
						if(src_column.equals(tar_column)){
						  if(!src_columns_list.get(i)[1].trim().equals(tar_columns_list.get(i)[1].trim())){ 
							  if(src_columns_list.get(i)[1].indexOf("varchar")!=-1&&tar_columns_list.get(i)[1].indexOf("varchar")!=-1){
								  replaceFlg=1;
								  pubAPIs.writerLog("字段类型varchar精度改变:" + tar_columns_list.get(i)[0]+"---"+tar_columns_list.get(i)[1]);
							  }else{
								  stmt2.close();
								  throw new PubException("源表"+src_column+"字段类型与目标表字段类型不一致,分別为："+src_columns_list.get(i)[1]+"-----"+tar_columns_list.get(i)[1]);
							  }
						  }
						}else{
							stmt2.close();
							throw new PubException("源表字段名与目标表字段名不一致");
						}
					}
					if(replaceFlg==1){
						  String replaceColStr = replaceColumn.toString();
						  replaceColStr = replaceColStr.substring(0,replaceColStr.length()-1);
						  String repstr="ALTER TABLE " +tranfersDataBean.getTab_full_name()+ " REPLACE COLUMNS("+replaceColStr+");";
						  stmt2.execute(repstr);
					}
					stmt2.close();	
				}
				if (metadataBean_txt.getResult() == -1|| !(metadataBean_txt.getColumnStr().replace("`", "")).equals(metadataBean.getColumnStr().replace("`", ""))) {
					int x =tranfersDataBean.getAlltxtField().split(",").length;//txt表字段个数
					if(l1>n1&&tranfersDataBean.getRemove_fields().equals("")){
						conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
						Statement stmt2= conn_tdh.createStatement();
						for (int k =n1; k < l1; k++) {
							String[] addColumn = src_columns_list.get(k);
							String addstr="ALTER TABLE " +tranfersDataBean.getTab_full_name()+ " ADD COLUMNS("+addColumn[0].replace("`", "")+ " " +addColumn[1] + ");";
							stmt2.execute(addstr);
							pubAPIs.writerLog("字段增加:" + addColumn[0].replace("`", "")+"---"+addColumn[1]);
						}
						stmt2.close();
					}else if(l1>n1&&!tranfersDataBean.getRemove_fields().equals("")&&n1<x){
						conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
						Statement stmt2= conn_tdh.createStatement();
						String tmp="";
						for(String[] insertField1:tar_columns_list){
							   tmp += insertField1[0]+",";
						}
						for (int k =x; k < l1; k++) {
							String[] addColumn = src_columns_list.get(k);
							String addstr="ALTER TABLE " +tranfersDataBean.getTab_full_name()+ " ADD COLUMNS("+addColumn[0].replace("`", "")+ " " +addColumn[1] + ");";
							stmt2.execute(addstr);
							 tmp += addColumn[0]+",";
							pubAPIs.writerLog("字段增加:" + addColumn[0].replace("`", "")+"---"+addColumn[1]);
						}
						insertField=tmp.substring(0,tmp.length()-1);
						tranfersDataBean.setInsertField(insertField);
						stmt2.close();
					}else if(l1>n1&&!tranfersDataBean.getRemove_fields().equals("")&&n1==x){
						String tmp="";
						for(String[] insertField1:tar_columns_list){
							   tmp += insertField1[0]+",";
						}
						insertField=tmp.substring(0,tmp.length()-1);
						tranfersDataBean.setInsertField(insertField);
					}
					else{
						throw new PubException(tabInfo_file_path + "文件内容存在问题或是源表与目标表字段不一致!");
					}
				}
				String add_sql_infor = "";
				File add_sql_file = new File(add_sql_file_path);
				if (add_sql_file.exists()) {
					add_sql_infor = FileUtils.readFileToString(add_sql_file);
				}
				if (!"".equals(add_sql_infor)) {
					add_sql_infor = add_sql_infor.replace(src_tab_full_name, tab_full_name);
					Statement stmt1 = conn_tdh.createStatement();
					pubAPIs.writerLog("add_sql_infor:" + add_sql_infor);
					stmt1.addBatch(add_sql_infor);
					stmt1.executeBatch();
					stmt1.clearBatch();
					stmt1.close();
				}

				String trun_sql_infor = "";
				File trun_sql_file = new File(trun_sql_file_path);
				if (trun_sql_file.exists()) {
					trun_sql_infor = FileUtils.readFileToString(trun_sql_file);
				}
				if (!"".equals(trun_sql_infor)) {
					trun_sql_infor = trun_sql_infor.replace(src_tab_full_name, tab_full_name);
					Statement stmt1 = conn_tdh.createStatement();
					pubAPIs.writerLog("trun_sql_infor:" + trun_sql_infor);
					stmt1.addBatch(trun_sql_infor);
					stmt1.executeBatch();
					stmt1.clearBatch();
					stmt1.close();
				}

				String tmpTabInfo_infor = "";
				File tmpTabInfo_file = new File(tmpTabInfo_file_path);
				if (tmpTabInfo_file.exists()) {
					tmpTabInfo_infor = FileUtils.readFileToString(tmpTabInfo_file);
				}
				if (!"".equals(tmpTabInfo_infor)) {
					Statement stmt1 = conn_tdh.createStatement();
					pubAPIs.writerLog("tmpTabInfo_infor:" + tmpTabInfo_infor);
					stmt1.addBatch(tmpTabInfo_infor);
					stmt1.executeBatch();
					stmt1.clearBatch();
					stmt1.close();
					File add_sql_file1 = new File(add_sql_file_path);
					if (add_sql_file1.exists()) {
						tmpTabInfo_infor = FileUtils.readFileToString(add_sql_file1);
						if (!"".equals(tmpTabInfo_infor)) {
							tmpTabInfo_infor = tmpTabInfo_infor.replace(src_tab_full_name, tmp_tab_full_name);
							Statement stmt2 = conn_tdh.createStatement();
							pubAPIs.writerLog("add_sql_infor:" + tmpTabInfo_infor);
							stmt2.addBatch(tmpTabInfo_infor);
							stmt2.executeBatch();
							stmt2.clearBatch();
							stmt2.close();
						}
					}

				}

				String tmp_table_location = "";
				String sql2 = "SELECT t.table_location||'/' as table_location FROM system.tables_v t WHERE t.database_name='"
						+ temp_database + "' AND table_name = '" + temp_tab_name + "';";
				Statement stmt2 = conn_tdh.createStatement();
				ResultSet rs2 = stmt2.executeQuery(sql2);
				while (rs2.next()) {
					tmp_table_location = rs2.getString("table_location");
				}
				rs2.close();
				stmt2.close();

				if (new File(data_file_path).exists()) {
					TransferFiles transferFiles = new TransferFiles("dataHandle");
					transferFiles.setOut_times(out_times);
					transferFiles.setPubAPIs_className(tab_full_name);
					transferFiles.transferCatalogToTDH("dataImport_login_kerberos", "dataImport_conf_tdh",
							data_file_path, tmp_table_location);
				}
				File min_sql_file = new File(min_sql_file_path);
				int l = 0;// 用于判断是否有分区
				if (min_sql_file.exists()) {
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(min_sql_file)));
					String lineTxt = null;
					while ((lineTxt = br.readLine()) != null) {
						if (lineTxt == null || "".equals(lineTxt.trim())) {
							continue;
						}
						l++;
						lineTxt = lineTxt.replace(src_tab_full_name, tab_full_name);
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
								DataImportThread dataImportThread = new DataImportThread(tranfersDataBean, metadataBean,
										lineTxt);
								dataImportThread.start();
								m++;
								break;
							}
							Thread.sleep(1000);
						}
					}
					if (l == 0) {// 空表或空分区
						TransferDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
						TransferDataPub.threadCheckDataImport(tab_full_name, "add2");
						TransferDataPub.threadCheckDataImport(tab_full_name, "minus");
					}
					br.close();
				}
			} else if ("auto-sql".equals(exp_type)) {
				MetadataTDHBean metadataBean_txt = metadataTDH.analysTdhMetadataFromTXT(tranfersDataBean, "all");
				
				ArrayList<String[]> src_columns_list = metadataBean_txt.getSrc_columns_list();
				ArrayList<String[]> tar_columns_list = metadataBean.getTar_columns_list();
				int n1 = tar_columns_list.size();//目标表字段个数
				int l1 =src_columns_list.size();//源表字段个数
				if(l1<n1){
					pubAPIs.writerLog("源表字段比目标表的字段少");
					throw new PubException("源表字段小于目标表字段");
				}
				if(l1==n1||l1>n1){
					Statement stmt2= conn_tdh.createStatement();
					StringBuffer replaceColumn = new StringBuffer();
					int replaceFlg=0;
					for(int i=0;i<n1;i++){
						String src_column = src_columns_list.get(i)[0].trim().toLowerCase().replace("`", "");
						String tar_column = tar_columns_list.get(i)[0].trim().toLowerCase().replace("`", "");
						replaceColumn.append(src_columns_list.get(i)[0].trim().replace("`", "")+" ");
						replaceColumn.append(src_columns_list.get(i)[1].trim()+",");
						if(src_column.equals(tar_column)){
						  if(!src_columns_list.get(i)[1].trim().equals(tar_columns_list.get(i)[1].trim())){ 
							  if(src_columns_list.get(i)[1].indexOf("varchar")!=-1&&tar_columns_list.get(i)[1].indexOf("varchar")!=-1){
								  replaceFlg=1;
								  pubAPIs.writerLog("字段类型varchar精度改变:" + tar_columns_list.get(i)[0].replace("`", "")+"---"+tar_columns_list.get(i)[1]);
							  }else{
								  stmt2.close();
								  throw new PubException("源表"+src_column+"字段类型与目标表字段类型不一致,分別为："+src_columns_list.get(i)[1]+"-----"+tar_columns_list.get(i)[1]);
							  }
						  }
						}else{
							stmt2.close();
							throw new PubException("源表字段名与目标表字段名不一致");
						}
					}
					if(replaceFlg==1){
						  String replaceColStr = replaceColumn.toString();
						  replaceColStr = replaceColStr.substring(0,replaceColStr.length()-1);
						  String repstr="ALTER TABLE " +tranfersDataBean.getTab_full_name()+ " REPLACE COLUMNS("+replaceColStr+");";
						  stmt2.execute(repstr);
					}
					stmt2.close();	
				}
				if (metadataBean_txt.getResult() == -1|| !(metadataBean_txt.getColumnStr().replace("`", "")).equals(metadataBean.getColumnStr().replace("`", ""))) {
					int x =tranfersDataBean.getAlltxtField().split(",").length;//txt表字段个数
					System.out.println(tranfersDataBean.getAlltxtField());
					if(l1>n1&&tranfersDataBean.getRemove_fields().equals("")){
						conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
						Statement stmt2= conn_tdh.createStatement();
						for (int k =n1; k < l1; k++) {
							String[] addColumn = src_columns_list.get(k);
							String addstr="ALTER TABLE " +tranfersDataBean.getTab_full_name()+ " ADD COLUMNS("+addColumn[0].replace("`", "")+ " " +addColumn[1] + ");";
							stmt2.execute(addstr);
							pubAPIs.writerLog("字段增加:" + addColumn[0].replace("`", "")+"---"+addColumn[1]);
						}
						stmt2.close();
					}else if(l1>n1&&!tranfersDataBean.getRemove_fields().equals("")&&n1<x){
						conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
						Statement stmt2= conn_tdh.createStatement();
						String tmp="";
						for(String[] insertField1:tar_columns_list){
							   tmp += insertField1[0]+",";
						}
						for (int k =x; k < l1; k++) {
							String[] addColumn = src_columns_list.get(k);
							String addstr="ALTER TABLE " +tranfersDataBean.getTab_full_name()+ " ADD COLUMNS("+addColumn[0].replace("`", "")+ " " +addColumn[1] + ");";
							stmt2.execute(addstr);
							 tmp += addColumn[0]+",";
							pubAPIs.writerLog("字段增加:" + addColumn[0].replace("`", "")+"---"+addColumn[1]);
						}
						insertField=tmp.substring(0,tmp.length()-1);
						tranfersDataBean.setInsertField(insertField);
						stmt2.close();
					}else if(l1>n1&&!tranfersDataBean.getRemove_fields().equals("")&&n1==x){
						String tmp="";
						for(String[] insertField1:tar_columns_list){
							   tmp += insertField1[0]+",";
						}
						insertField=tmp.substring(0,tmp.length()-1);
						tranfersDataBean.setInsertField(insertField);
					}
					else{
						throw new PubException(tabInfo_file_path + "文件内容存在问题或是源表与目标表字段不一致!");
					}
				}
				String add_sql_infor = "";
				File add_sql_file = new File(add_sql_file_path);
				if (add_sql_file.exists()) {
					add_sql_infor = FileUtils.readFileToString(add_sql_file);
				}
				if (!"".equals(add_sql_infor)) {
					add_sql_infor = add_sql_infor.replace(src_tab_full_name, tab_full_name);
					Statement stmt1 = conn_tdh.createStatement();
					pubAPIs.writerLog("add_sql_infor:" + add_sql_infor);
					stmt1.addBatch(add_sql_infor);
					stmt1.executeBatch();
					stmt1.clearBatch();
					stmt1.close();
				}

				String tmpTabInfo_infor = "";
				File tmpTabInfo_file = new File(tmpTabInfo_file_path);
				if (tmpTabInfo_file.exists()) {
					tmpTabInfo_infor = FileUtils.readFileToString(tmpTabInfo_file);
				}
				if (!"".equals(tmpTabInfo_infor)) {
					Statement stmt1 = conn_tdh.createStatement();
					pubAPIs.writerLog("tmpTabInfo_infor:" + tmpTabInfo_infor);
					stmt1.addBatch(tmpTabInfo_infor);
					stmt1.executeBatch();
					stmt1.clearBatch();
					stmt1.close();
				}

				String tmp_table_location = "";
				String sql2 = "SELECT t.table_location||'/' as table_location FROM system.tables_v t WHERE t.database_name='"
						+ temp_database + "' AND table_name = '" + temp_tab_name + "';";
				Statement stmt2 = conn_tdh.createStatement();
				ResultSet rs2 = stmt2.executeQuery(sql2);
				while (rs2.next()) {
					tmp_table_location = rs2.getString("table_location");
				}
				rs2.close();
				stmt2.close();

				if (new File(data_file_path).exists()) {
					TransferFiles transferFiles = new TransferFiles("dataHandle");
					transferFiles.setOut_times(out_times);
					transferFiles.setPubAPIs_className(tab_full_name);
					transferFiles.transferCatalogToTDH("dataImport_login_kerberos", "dataImport_conf_tdh",
							data_file_path, tmp_table_location);
				}

				File min_sql_file = new File(min_sql_file_path);
				int l = 0;// 用于判断是否有分区
				if (min_sql_file.exists()) {
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(min_sql_file)));
					String lineTxt = null;
					while ((lineTxt = br.readLine()) != null) {
						if (lineTxt == null || "".equals(lineTxt.trim())) {
							continue;
						}
						l++;
						lineTxt = lineTxt.replace(src_tab_full_name, tab_full_name);
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
								DataImportThread dataImportThread = new DataImportThread(tranfersDataBean, metadataBean,
										lineTxt);
								dataImportThread.start();
								m++;
								break;
							}
							Thread.sleep(1000);
						}
					}
					if (l == 0) {// 空表或空分区
						TransferDataPub.threadLock(lock_table, "minus", tab_full_name, 0);
						TransferDataPub.threadCheckDataImport(tab_full_name, "add2");
						TransferDataPub.threadCheckDataImport(tab_full_name, "minus");
					}
					br.close();
				}

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
			String drop_infor = "";
			File drop_file = new File(drop_file_path);
			if (drop_file.exists()) {
				drop_infor = FileUtils.readFileToString(drop_file);
			}
			pubAPIs.writerLog("drop_infor:" + drop_infor);
			if (!"".equals(drop_infor)) {
				Statement stmt3 = conn_tdh.createStatement();
				stmt3.addBatch(drop_infor);
				stmt3.executeBatch();
				stmt3.clearBatch();
				stmt3.close();
			}
			int result1 = TransferDataPub.threadCheckDataImport(tab_full_name, "get")[1];
			if (result1 != m) {
				String errorInfo = tab_full_name + "表落地失败!" + result1 + "|" + m;
				throw new PubException(errorInfo);
			}
			TransferDataPub.write_imp_jobLog(tranfersDataBean, "2", "作业结束");
			m++;
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			TransferDataPub.write_imp_jobLog(tranfersDataBean, "1", eStr);
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

	private class DataImportThread extends Thread {
		private PubAPIs pubAPIs = new PubAPIs("dataHandle");
		private Connection conn_tdh = null;
		private TranfersDataBean tranfersDataBean = new TranfersDataBean();
		private MetadataTDHBean metadataBean = new MetadataTDHBean();
		private String lineTxt = "";
		private String tab_full_name = "";
		private String isRangePartition = "";
		private String isPartition = "";
		private String lock_table = "";

		private DataImportThread(TranfersDataBean tranfersDataBean, MetadataTDHBean metadataBean, String lineTxt)
				throws Exception {
			BeanUtils.copyProperties(this.tranfersDataBean, tranfersDataBean);
			BeanUtils.copyProperties(this.metadataBean, metadataBean);
			this.lineTxt = lineTxt;
			this.isRangePartition = metadataBean.getIsRangePartition();
			this.isPartition = metadataBean.getIsPartition();
			this.lock_table = tranfersDataBean.getLock_table();
			this.tab_full_name = tranfersDataBean.getTab_full_name();
		}

		public void run() {
			pubAPIs.writerLog("T->" + TransferDataPub.threadLock(lock_table, "get", tab_full_name, 0) + " times ");
			conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
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
					if (conn_tdh != null && !conn_tdh.isClosed()) {
						conn_tdh.close();
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
			try {
				String[] lineTxtS = lineTxt.split("\001");
				String sql_del = lineTxtS[0].trim();
				String insertField=tranfersDataBean.getInsertField();
				if ("auto-sql".equals(exp_type)) {
					sql_del = "delete from " + metadataBean.getTab_full_name() + " where " + sql_del;
					Statement stmt1 = conn_tdh.createStatement();
					stmt1.execute(sql_del);
					stmt1.close();
				}
				Statement stmt0 = conn_tdh.createStatement();
				String sql = lineTxtS[1].trim();
				if ("false".equals(isRangePartition) && "true".equals(isPartition)) {
					if(!tranfersDataBean.getRemove_fields().equals("")&&tranfersDataBean.getRemove_fields()!=null&&!insertField.equals("")){
						sql = "insert into "+ tab_full_name + metadataBean.getPartition_insert_head()+"(" +insertField+")" + " select  " +insertField+" "+ sql;
					}else{
						if(metadataBean.getPartition_insert_head().indexOf(",ORACLE)") != -1 ) {
							sql = "insert into " + tab_full_name +" "+ metadataBean.getPartition_insert_head().replace(",ORACLE", "") + " select *  " + sql;
						}else {
							sql = "insert into " + tab_full_name +" "+ metadataBean.getPartition_insert_head() + " select *  " + sql;
						}
					}
					
				} else {
					if(!(tranfersDataBean.getRemove_fields()).equals("")&&tranfersDataBean.getRemove_fields()!=null&&!insertField.equals("")){
						sql = "insert into "+ tab_full_name +"("+insertField +")" + " select "+insertField+" "+  sql;
					}
					else{
						sql = "insert into " + tab_full_name + " select * " + sql;
					}
				}
				pubAPIs.writerLog("sql:" + sql);
				if (!"".equals(sql)) {
					stmt0.execute("set ngmr.partition.automerge=true");
					stmt0.execute("set hive.exec.dynamic.partition=true");
 					stmt0.execute(sql);
					stmt0.close();
				}
				conn_tdh.close();
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog("error:" + tab_full_name + "\n" + eStr);
				return -1;
			}
			return result;
		}
	}

}
