package cn.com.dataHandle.metadataHandle;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import cn.com.dataHandle.EtlDataPub;
import cn.com.dataHandle.bean.MetadataOCABean;

import cn.com.dataHandle.bean.TranfersDataBean;
import cn.com.pub.PubAPIs;

public class MetadataOCA {
	private static PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private static Connection conn_mysql = null;
	private static Connection conn_oracle=null;
	private Connection conn_tdh = null;
	private String log_file_dir = "";
	private String lastChar="";
	EtlDataPub etlDataPub = new EtlDataPub();
	
	public MetadataOCABean analysOCAMetadataFromTDH(TranfersDataBean tranfersDataBean)
			throws Exception {
		MetadataOCABean metadataBean = new MetadataOCABean();
		String tab_full_name = tranfersDataBean.getTab_full_name();
		String etl_date=tranfersDataBean.getEtl_date();
		lastChar=etlDataPub.getDirSign();
		String data_path=tranfersDataBean.getHome_path()+etl_date+lastChar+tab_full_name+lastChar+"data"+lastChar;
		log_file_dir = tranfersDataBean.getLog_file_dir();
		PubAPIs.setLogsPath(log_file_dir + "TranfersDataExp" + PubAPIs.getDirSign());
		PubAPIs.setErrorLogsPath(log_file_dir + "TranfersDataExpError" + PubAPIs.getDirSign());
		pubAPIs.setClassName(tab_full_name);
		pubAPIs.writerLog("Run analysOCAMetadataFromTDH:" + tab_full_name);
		ArrayList<String[]> src_columns_list = new ArrayList<String[]>();//源表字段和字段类型
		try {
			String createTableSQL = "";
			//String crtlSQL = "";
			String exec_type=tranfersDataBean.getExec_type();
			String table_full_name=tranfersDataBean.getTab_full_name();
			String table_name =tranfersDataBean.getTab_name();
			conn_mysql = EtlDataPub.createMysqlConnSM4("dataExport_mysql_login_ldap");
			Statement stmt1 = conn_mysql.createStatement();
			String sql_mysql1 = "select t.src_columns,t.src_fieldtype,t.comment_on_column from dataload.dataload_table_info t where t.table_full_name_orc = '"
					+ table_full_name + "' order by t.columns_order";
			pubAPIs.writerLog("sql_mysql1:" + sql_mysql1);
			ResultSet rs_mysql1 = stmt1.executeQuery(sql_mysql1);
			String create_tb_sql = "create table " + table_name + "(" +"\t\n";
			// crtlSQL = "LOAD DATA "  +"\t\n";
			 System.out.println("data_path"+data_path);
				//String[] list= etlDataPub.checkfile(data_path);
				 
//				for(int i=0;i<list.length;i++) {
//					String filr=data_path+list[i];
//					System.out.println("filr----------"+filr);
//					File file=new File(filr);
//					file.renameTo(new File(filr+".txt")); //改名
//					crtlSQL +="INFILE "+ "'"+data_path+list[i]+"'"+"\t\n";
//				}
//				if("all".equals(exec_type)) {
//					crtlSQL+="TRUNCATE INTO TABLE " + table_name+"\t\n";
//							
//				}else if("add".equals(exec_type)) {
//					crtlSQL+="APPEND INTO TABLE " + table_name+"\t\n";
//				}else {
//					crtlSQL+="INSERT INTO TABLE " + table_name+"\t\n";
//				}
//				crtlSQL+="Fields terminated by \"#&\"" +"\t\n";
//				crtlSQL+="(";
			while (rs_mysql1.next()) {
				String s = rs_mysql1.getString(1);
				String s1 = rs_mysql1.getString(2);
				String s2 = rs_mysql1.getString(3);
				create_tb_sql += s + " " + s1 + "," + "\t\n";
				//crtlSQL += s+",";
				
				String[] columns_info_tar = new String[] { s, s1 };
				src_columns_list.add(columns_info_tar);
			}
            // crtlSQL += "bucket_id_tdh,system_id_tdh,etl_date_tdh)";
             //crtlSQL.replace();
             
			create_tb_sql += "bucket_id_tdh varchar2(50)," + "\t\n";
			create_tb_sql += "system_id_tdh varchar2(50)," + "\t\n";
			create_tb_sql +=  "etl_date_tdh varchar2(50)" + "\t\n";
			create_tb_sql +=  ");";
			
			String drop_tb_sql ="drop table " + table_name +";";
			String trun_tb_sql ="truncate table " + table_name +";";
			metadataBean.setCreateTableSQL(create_tb_sql);
			metadataBean.setDropTabSQL(drop_tb_sql);
			metadataBean.setTruncateTabSQL(trun_tb_sql);
			//metadataBean.setCrtlSQL(crtlSQL);
			metadataBean.setSrc_columns_list(src_columns_list);
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			if (eStr.indexOf("Table not found:") != -1) {
				metadataBean.setResult(-1);
				return metadataBean;
			} else {
				throw e;
			}
		} finally {
			if (conn_mysql != null && !conn_mysql.isClosed()) {
				conn_mysql.close();
			}
		}
		return metadataBean;
	}
	public MetadataOCABean analysOCACTLFromTDH(TranfersDataBean tranfersDataBean)
			throws Exception {
		MetadataOCABean  ctldataBean = new MetadataOCABean();
		String tab_full_name = tranfersDataBean.getTab_full_name();
		String etl_date=tranfersDataBean.getEtl_date();
		lastChar=etlDataPub.getDirSign();
		String data_path=tranfersDataBean.getHome_path()+etl_date+lastChar+tab_full_name+lastChar+"data"+lastChar;
		log_file_dir = tranfersDataBean.getLog_file_dir();
		PubAPIs.setLogsPath(log_file_dir + "TranfersDataExp" + PubAPIs.getDirSign());
		PubAPIs.setErrorLogsPath(log_file_dir + "TranfersDataExpError" + PubAPIs.getDirSign());
		pubAPIs.setClassName(tab_full_name);
		pubAPIs.writerLog("Run analysOCACTLFromTDH:" + tab_full_name);
		ArrayList<String[]> src_columns_list = new ArrayList<String[]>();//源表字段和字段类型
		try {
			String crtlSQL = "";
			String exec_type=tranfersDataBean.getExec_type();
			String table_full_name=tranfersDataBean.getTab_full_name();
			String table_name =tranfersDataBean.getTab_name();
			conn_mysql = EtlDataPub.createMysqlConnSM4("dataExport_mysql_login_ldap");
			Statement stmt1 = conn_mysql.createStatement();
			String sql_mysql1 = "select t.src_columns,t.src_fieldtype,t.comment_on_column from dataload.dataload_table_info t where t.table_full_name_orc = '"
					+ table_full_name + "' order by t.columns_order";
			pubAPIs.writerLog("sql_mysql1:" + sql_mysql1);
			ResultSet rs_mysql1 = stmt1.executeQuery(sql_mysql1);
			 crtlSQL = "LOAD DATA "  +"\t\n";
			 System.out.println("data_path"+data_path);
				String[] list= etlDataPub.checkfile(data_path);
				 
				for(int i=0;i<list.length;i++) {
					String filr=data_path+list[i];
					System.out.println("filr----------"+filr);
					File file=new File(filr);
					//file.renameTo(new File(filr+".txt")); //改名
					crtlSQL +="INFILE "+ "'"+data_path+list[i]+"'"+"\t\n";
				}
				if("all".equals(exec_type)) {
					crtlSQL+="TRUNCATE INTO TABLE " + table_name+"\t\n";
							
				}else if("add".equals(exec_type)) {
					crtlSQL+="APPEND INTO TABLE " + table_name+"\t\n";
				}else {
					crtlSQL+="INSERT INTO TABLE " + table_name+"\t\n";
				}
				crtlSQL+="Fields terminated by \"#&\"" +"\t\n";
				crtlSQL+="(";
			while (rs_mysql1.next()) {
				String s = rs_mysql1.getString(1);
				String s1 = rs_mysql1.getString(2);
				String s2 = rs_mysql1.getString(3);
				crtlSQL += s+",";
			}
             crtlSQL += "bucket_id_tdh,system_id_tdh,etl_date_tdh)";
             //crtlSQL.replace();
			ctldataBean.setCrtlSQL(crtlSQL);
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			if (eStr.indexOf("Table not found:") != -1) {
				ctldataBean.setResult(-1);
				return ctldataBean;
			} else {
				throw e;
			}
		} finally {
			if (conn_mysql != null && !conn_mysql.isClosed()) {
				conn_mysql.close();
			}
		}
		return ctldataBean;
	}
	
	public  MetadataOCABean analysOCAMetadataFromDB(TranfersDataBean tranfersDataBean)
			throws Exception { 
		MetadataOCABean metadataBean = new MetadataOCABean();		 
		try {
			conn_oracle = EtlDataPub.createOracleConnSM4("dataImport_oracle_login_ldap");
			Statement stmt1 = conn_oracle.createStatement();
			String sql1="select column_name,data_type,data_length from user_tab_columns WHERE TABLE_name=upper('ciyutest')";
			 ResultSet rs = stmt1.executeQuery(sql1);
			 ArrayList<String[]> tar_columns_list = new ArrayList<String[]>(); //目标表字段和字段类型
			 while(rs.next()) {
				 String s1=rs.getString(1);
				 String s2=rs.getString(2);
				 String s3=rs.getString(3);
				 System.out.println("目标表："+s1+"---"+s2+"("+s3+")");
				 String[] columns_info = new String[] { s1,s2+"("+s3+")" };
				 tar_columns_list.add(columns_info);
			 }
			 metadataBean.setTar_columns_list(tar_columns_list);
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			if (eStr.indexOf("Table not found:") != -1) {
				metadataBean.setResult(-1);
				return metadataBean;
			} else {
				throw e;
			}
		} finally {
			if (conn_oracle != null && !conn_oracle.isClosed()) {
				conn_oracle.close();
			}
		}
		return metadataBean;
		
		 
	}
	
}
