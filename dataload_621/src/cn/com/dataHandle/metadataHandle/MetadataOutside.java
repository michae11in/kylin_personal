package cn.com.dataHandle.metadataHandle;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.com.dataHandle.DataLoadPub;
import cn.com.dataHandle.bean.DataLoadBean;
import cn.com.dataHandle.bean.MetadataOutsideBean;
import cn.com.pub.PubAPIs;
import cn.com.pub.PubException;

public class MetadataOutside {
	private PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private Connection conn_mysql = null;

	public MetadataOutsideBean oracleMetadataAnalysis(DataLoadBean dataLoadBean) throws Exception {
		MetadataOutsideBean metadataBean = new MetadataOutsideBean();
		try {
			FileSystem fs = DataLoadPub.getFileSystem(dataLoadBean.getConf_hdfs(), dataLoadBean.getLogin_kerberos());
			// 得到SQL文件中ORACLE文件的字段信息
			Path p = new Path(dataLoadBean.getDatafile_hist_sql_file());
			FSDataInputStream fsr = fs.open(p);
			//BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			InputStreamReader isr = new InputStreamReader(fsr,"UTF-8"); //改
			BufferedReader bufferedReader =new BufferedReader(isr); // 改
			String lineTxt = "";
			int sign = 0;
			int n = 0;
			ArrayList<String[]> src_list_bef = new ArrayList<String[]>();// 上一次文件表中的字段信息列表
			ArrayList<String[]> src_list_curr = new ArrayList<String[]>();// 文件表中的字段信息列表
			ArrayList<String> insert_table_info_list = new ArrayList<String>();
			HashMap<String,String> comment_info = new HashMap<String,String>();
			String primaryKey = "";
			//pubAPIs.writerLog("===========0");
			while ((lineTxt = bufferedReader.readLine()) != null) {
				lineTxt = lineTxt.trim();
				lineTxt = lineTxt.replaceAll("\\s+", " ");
				lineTxt = lineTxt.replaceAll("\\((\\d+)(\\,)(\\d+)\\)", "($1-$3)");
				if ("".equals(lineTxt)) {
					continue;
				}
				lineTxt = lineTxt.toLowerCase();
				if (sign == 0 && lineTxt.indexOf("(") != -1) {
					sign = 1;
					continue;
				}
				if (sign == 1 && (");".equals(lineTxt.replace(" ", "")) || ")".equals(lineTxt)
						|| lineTxt.replace(" ", "").indexOf("primarykey") != -1)) {
					sign = 2;
					String temp = lineTxt.replace(" ", "");
					if (temp.indexOf("primarykey") != -1) {
						primaryKey = temp.replace("primarykey", "").replace("'", "");
						primaryKey = primaryKey.replace("(", "").replace(")", "");
					}
				}
				if (sign == 1) {
					lineTxt = lineTxt.substring(0,
							lineTxt.lastIndexOf(",") != -1 ? lineTxt.lastIndexOf(",") : lineTxt.length());
					System.out.println(lineTxt);
					String comment = "";
					lineTxt = lineTxt.replaceAll("\\((\\d+)(-)(\\d+)\\)", "($1,$3)");
					String lineTxts[] = lineTxt.split(" ");
					String[] columns_info_curr = new String[] { lineTxts[0], lineTxts[1], comment };
					src_list_curr.add(columns_info_curr);
					String columns_order = (1000 + n) + "";
					String tableInfo_insert_sql = "insert into dataload_table_info (id,job_id,table_full_name_orc,src_columns,src_fieldtype,comment_on_column,columns_order,date_change) values (";
					tableInfo_insert_sql += "'" + UUID.randomUUID().toString().replace("-", "").replace(".", "") + "','"
							+ dataLoadBean.getJob_id() + "','" + dataLoadBean.getTable_full_name_orc() + "','"
							+ lineTxts[0] + "','" + lineTxts[1] + "','" + comment + "','" + columns_order + "','"
							+ dataLoadBean.getEtl_date() + "')";
					insert_table_info_list.add(tableInfo_insert_sql);
					n++;
				}
				int a = lineTxt.indexOf("comment on table");
				int b = lineTxt.indexOf("comment on column");
				if(a!=-1) {
					int a1=lineTxt.indexOf(" is");
					int a2=lineTxt.length()-1;
					String table_name= lineTxt.substring(a+16, a1+1);
					String table_comment= lineTxt.substring(a1+4, a2);
					comment_info.put(table_name.trim(), table_comment.trim());
				}
				if (b!=-1) {
					int b1=lineTxt.indexOf(" is");
					int b2=lineTxt.length()-1;
					String column_name= lineTxt.substring(b+17, b1+1);
					String column_comment= lineTxt.substring(b1+4, b2);
					comment_info.put(column_name.trim(), column_comment.trim());
				}
				
			}
			metadataBean.setPrimaryKey(primaryKey);
			// 得到Mysql表中上一次ORACLE文件的字段信息
			//pubAPIs.writerLog("===========1");
			conn_mysql = DataLoadPub.createMysqlConnSM4(dataLoadBean.getAuthUserMysql());
			///pubAPIs.writerLog("===========2");
			Statement stmt_mysql1 = conn_mysql.createStatement();
			String sql_mysql1 = "select t.src_columns,t.src_fieldtype,t.comment_on_column from dataload_table_info t where  t.table_full_name_orc='" + dataLoadBean.getTable_full_name_orc()
                                + "' order by t.columns_order";
			pubAPIs.writerLog("sql_mysql1:" + sql_mysql1);
			ResultSet rs_mysql1 = stmt_mysql1.executeQuery(sql_mysql1);

			int m = 0;
			while (rs_mysql1.next()) {
				String src_columns = rs_mysql1.getString("src_columns").trim().toLowerCase();
				String src_fieldtype = rs_mysql1.getString("src_fieldtype").trim().toLowerCase();
				String comment_on_column = rs_mysql1.getString("comment_on_column").trim().toLowerCase();
				String[] columns_info_bef = new String[] { src_columns, src_fieldtype, comment_on_column };
				src_list_bef.add(columns_info_bef);
				if (src_list_curr.size() < m) {
					throw new PubException(dataLoadBean.getTable_full_name_orc() + "表上游系统字段减少了!");
				} else {
					String columnsInfo[] = src_list_curr.get(m);
					if (src_columns == null || !src_columns.equals(columnsInfo[0])) {// 抛出异常转人工处理
						throw new PubException(dataLoadBean.getTable_full_name_orc() + "表从" + m + "个字段名称开始不一致，当前字段名称为"
								+ src_columns + ",原字段名称为" + columnsInfo[0]);
					}
					if (src_fieldtype == null || !src_fieldtype.equals(columnsInfo[1])) {// 抛出异常转人工处理
						throw new PubException(dataLoadBean.getTable_full_name_orc() + "表从" + m + "个字段类型开始不一致，当前字段类型为"
								+ src_fieldtype + ",原字段类型为" + columnsInfo[1]);
					}
				}
				m++;
			}
			rs_mysql1.close();
			stmt_mysql1.close();
			metadataBean.setInsert_table_info_list(insert_table_info_list);
			metadataBean.setSrc_list_bef(src_list_bef);
			metadataBean.setSrc_list_curr(src_list_curr);
			metadataBean.setBef_size(m);
			metadataBean.setCur_size(n);
			metadataBean.setComment_info(comment_info);
		} catch (Exception e) {
			throw e;
		} finally {
			if (conn_mysql != null && !conn_mysql.isClosed()) {
				conn_mysql.close();
			}
		}
		return metadataBean;
	}
}
