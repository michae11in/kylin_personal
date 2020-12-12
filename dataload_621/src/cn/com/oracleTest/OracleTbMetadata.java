package cn.com.oracleTest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.com.dataHandle.DataLoadPub;
import cn.com.dataHandle.TransferDataPub;
import cn.com.dataHandle.bean.DataLoadBean;
import cn.com.dataHandle.bean.MetadataOutsideBean;
import cn.com.dataHandle.bean.TranfersDataBean;
import cn.com.pub.PubAPIs;
import cn.com.pub.PubException;

public class OracleTbMetadata {
	private static PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private static Connection conn_mysql1 = null;

	public static synchronized void ciyutest(String tabInfo_file_path, String tab_name, String table_full_name)
			throws Exception {
		try {// 输出建表语句到卸数目录
			String file_path = tabInfo_file_path + tab_name + ".sql";
			// String file_path="E:\\CIYU.sql";
			// String tab_name1="rmis_z_loan_duebill_info";
			// String table_full_name1="'bod_datalake.t_rmis_z_loan_duebill_info_torc'";
			File outfile = new File(file_path);
			OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outfile), "utf-8");
			BufferedWriter bufferWriter = new BufferedWriter(writer);
			List<String> list = new ArrayList<>();
			conn_mysql1 = DataLoadPub.createMysqlConnSM4("dataExport_mysql_login_ldap");
			Statement stmt_mysql1 = conn_mysql1.createStatement();
			String sql_mysql1 = "select t.src_columns,t.src_fieldtype,t.comment_on_column from dataload_table_info t where t.table_full_name_orc ="
					+ table_full_name + " order by t.columns_order";
			pubAPIs.writerLog("sql_mysql1:" + sql_mysql1);
			ResultSet rs_mysql1 = stmt_mysql1.executeQuery(sql_mysql1);
			String create_tb = "create table" + tab_name + "(";
			list.add(create_tb + "\t\n");
			while (rs_mysql1.next()) {
				String s = rs_mysql1.getString(1);
				String s1 = rs_mysql1.getString(2);
				String s2 = rs_mysql1.getString(3);
				// System.out.println(s+"---"+s1+"--"+s2);
				list.add(s + " " + s1 + "," + "\t\n");

			}
			list.add("bucket_id_tdh varchar2(50)," + "\t\n");
			list.add("system_id_tdh varchar2(50)," + "\t\n");
			list.add("etl_date_tdh varchar2(50)" + "\t\n");
			list.add(");");
			// int i=0;
			// while(i<list.size()) {
			// //System.out.println("i:::"+i);
			// if(");".equals(list.get(i))) {
			// String invalue=list.get(i-1).replace("),",")");
			// list.set(i-1, invalue);
			// //System.out.println(list.get(i-1));
			//
			// }
			// i++;
			// }
			int j = 0;
			while (j < list.size()) {
				// System.out.println("j:::"+j);
				// System.out.println(list.get(j));
				bufferWriter.write(list.get(j));
				j++;
			}

			System.out.println("newlist:::::" + list);
			bufferWriter.flush();
			bufferWriter.close();

		} catch (Exception e) {
			throw e;
		} finally {
			try {
				if (conn_mysql1 != null && !conn_mysql1.isClosed()) {
					conn_mysql1.close();
				}
			} catch (SQLException e) {

				e.printStackTrace();
			}
		}

	}
}
