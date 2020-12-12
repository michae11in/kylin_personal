package cn.com.dataHandle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import cn.com.dataHandle.bean.DataLoadBean;
import cn.com.dataHandle.bean.MetadataOutsideBean;
import cn.com.dataHandle.metadataHandle.MetadataOutside;
import cn.com.pub.PubAPIs;
import cn.com.pub.PubException;

public class DataLoadMain {
	private PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private Connection conn_mysql = null;
	private Connection conn_tdh = null;
	private String thread_id = "";
	private FileSystem fs = null;
	private String authUserMysql = "";
	private int bucket_number_default = 60;// 默认分桶个数
	private int bucket_size_default = 70;// 默认分桶大小
	private int parition_days_default = 49;// 默认分区天数
	private int bucket_number_max = 60;// 最大分桶大小
	private int compression_ratio = 10;// 压缩比

	public int dataHandle(DataLoadBean dataLoadBean) {
		parition_days_default = Integer.parseInt(pubAPIs.getProperty("parition_days_default"));
		compression_ratio = Integer.parseInt(pubAPIs.getProperty("compression_ratio"));
		bucket_size_default = dataLoadBean.getInit_bucket_size();
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
		long start1 = new Date().getTime();
		dataLoadBean.setStartTime(start1);
		long tatol1 = 0;
		int result = 0;
		PubAPIs.setLogsPath(dataLoadBean.getLog_file_dir());
		// String tab_full_name = dataLoadBean.getTable_full_name_orc();
		pubAPIs.setClassName(dataLoadBean.getTable_full_name_orc());
		thread_id = dataLoadBean.getThread_id();
		String etl_date = dataLoadBean.getEtl_date();
		String table_name = dataLoadBean.getTable_name();
		String load_type = dataLoadBean.getLoad_type();
		String load_condition = dataLoadBean.getLoad_condition();
		String usr_def_partition = dataLoadBean.getUsr_def_partition();
		String src_table_name = dataLoadBean.getTable_name().trim().toLowerCase();
		load_condition = load_condition != null ? load_condition.trim() : "";
		try {
			authUserMysql = "dataLoad_mysql_login_ldap";
			conn_mysql = DataLoadPub.createMysqlConnSM4(authUserMysql);
			Statement stmt0 = conn_mysql.createStatement();
			String sql_ifRun = "select 1 from dataload_job_log  where table_name = '" + table_name + "' and etl_date= '"
					+ etl_date + "' and task_status='running';";
				ResultSet rs_ifRun = stmt0.executeQuery(sql_ifRun);				
			   if (rs_ifRun.next()) {
				pubAPIs.writerLog("其他批次正在加载 " + table_name + " 表，跳出本次加载！");
				rs_ifRun.close();
				return result;
			     }
			   
			DataLoadPub.write_jobLog(dataLoadBean, "0", "作业开始");
			int src_mun = DataLoadPub.getSrcTabSize(dataLoadBean);
			dataLoadBean.setJob_src_count(src_mun);
			fs = PubAPIs.getFileSystem(dataLoadBean.getConf_hdfs(), dataLoadBean.getLogin_kerberos(), "dataHandle",
					"get", null);
			conn_tdh = DataLoadPub.createTDHConnSM4(dataLoadBean.getAuthUserTDH());
			Statement stmt_tdh1 = conn_tdh.createStatement();
			String sql_tdh1 = "select t.database_name,t.table_name,t.table_location,max(t2.bucket_number) as bucket_number,max(partition_name) as partition_name,substring(max(t1.partition_range),0,8) as max_partition_start,substring(max(t1.partition_range),10,8) as max_partition_end,substring(min(t1.partition_range),10,8) as min_partition from system.tables_v t LEFT JOIN system.range_partitions_v t1 ON t.database_name = t1.database_name AND t.table_name=t1.TABLE_name AND t1.partition_range NOT LIKE 'M%' LEFT JOIN system.buckets_v t2 ON t.database_name = t2.database_name AND t.table_name=t2.TABLE_name WHERE t.database_name ='"
					+ dataLoadBean.getDb_name_orc() + "' AND t.table_name = '" + dataLoadBean.getTable_name_orc()
					+ "' group by t.database_name,t.table_name,t.table_location ";
			pubAPIs.writerLog("sql_tdh1:" + sql_tdh1);
			ResultSet rs_tdh1 = stmt_tdh1.executeQuery(sql_tdh1);
			String max_partition_end = "";
			String max_partition_start = "";
			String max_partition_name = "";
			String table_location = "";
			String next_partition_date = "";
			String last_partition_ENDdate = "";
			boolean existsTarTab = false;
			while (rs_tdh1.next()) {
				existsTarTab = true;
				table_location = rs_tdh1.getString("table_location");
				max_partition_name = rs_tdh1.getString("partition_name");
				bucket_number_default = rs_tdh1.getInt("bucket_number");
				max_partition_end = rs_tdh1.getString("max_partition_end");
				max_partition_end = max_partition_end != null ? max_partition_end : "";
				max_partition_start = rs_tdh1.getString("max_partition_start");
				max_partition_start = max_partition_start != null ? max_partition_start : "";
				if ("MINVALUE".equals(max_partition_start)) {
					max_partition_start = max_partition_end;
				}
			}
			rs_tdh1.close();
			stmt_tdh1.close();
			conn_tdh.close();
			// System.out.println(dataLoadBean.getDatafile_hist_txt_file());
			double txtFileSize = DataLoadPub.getFileSize(dataLoadBean.getDatafile_hist_txt_file(), fs);// 得到TXT表的大小
			txtFileSize = txtFileSize / 1024 / 1024 / compression_ratio;// 单位转换为M，10为压缩比

			int day_num = 0;
			if ("add".equals(load_type) && !dataLoadBean.getUsr_def_partition().equals("")
					&& dataLoadBean.getUsr_def_partition() != null) {
				if (existsTarTab) {
					double max_partition_size = DataLoadPub.getFileSize(table_location + "/" + max_partition_name, fs);// 得到上个分区的大小
					// max_partition_size = max_partition_size / 1024 / 1024/compression_ratio;//
					// 单位转换为M（不需要除压缩比）
					max_partition_size = max_partition_size / 1024 / 1024;// 单位转换为M
					if (max_partition_size == 0) {
						max_partition_size = txtFileSize;
					}
					Calendar c1 = Calendar.getInstance();
					Calendar c2 = Calendar.getInstance();
					Calendar c3 = Calendar.getInstance();
					c1.setTime(sdf1.parse(max_partition_start));
					c2.setTime(sdf1.parse(max_partition_end));
					c3.setTime(sdf1.parse(etl_date));
					long time1 = c1.getTimeInMillis();
					long time2 = c2.getTimeInMillis();
					long between_days = (time2 - time1) / (1000 * 3600 * 24);
					day_num = Integer.parseInt(String.valueOf(between_days));// 得到上个分区的天数
					double one_day_size = max_partition_size * 1.0 / (day_num + 1) * 1.0;// 得到上一个分区平均每天数据量大小 加1是为了避免除数为0
					long partition_size_default = bucket_number_default * bucket_size_default;
					int day_num_curr = (int) (partition_size_default / one_day_size) + 1;
					if (day_num_curr > 365 * 30) {
						day_num_curr = 365 * 30;
					}
					c3.add(Calendar.DATE, day_num_curr);
					next_partition_date = sdf1.format(c3.getTime());// 获取新建分区的日期
				}
			}

			if (existsTarTab && (!"".equals(max_partition_end)) && "merge".equals(load_type)) {
				throw new PubException(dataLoadBean.getTable_full_name_orc() + "merge模式不能处理分区表，请使用update模式");
			}
			MetadataOutside outsideMetadata = new MetadataOutside();
			MetadataOutsideBean metadataBean = outsideMetadata.oracleMetadataAnalysis(dataLoadBean); // 解析元数据
			if (",merge,update,".indexOf("," + load_type + ",") != -1) {
				String primaryKey = metadataBean.getPrimaryKey();
				String condition = dataLoadBean.getLoad_condition();
				if (!condition.equals(primaryKey)) {
					String warn_info = "警告:load_condition与primaryKey不一致，load_condition:" + condition + "->primaryKey:"
							+ primaryKey;
					pubAPIs.writerLog(warn_info);
					DataLoadPub.write_warnInfo(dataLoadBean, "column", "3", warn_info);
				}
			}

			ArrayList<String[]> tar_list = new ArrayList<String[]>();// 目标orc表中的字段信息列表
			boolean isFirst = (metadataBean.getBef_size() == 0 && metadataBean.getCur_size() > 0);
			if (isFirst) {// 表示此表第一次通过本系统入库，需要将表信息入库,然后判断一下对应ORC表是否存在，如果不存在就自动创建表，如果存在则抛出异常转人工处理
				if (existsTarTab) {
					throw new PubException(
							dataLoadBean.getTable_name() + "表,在dataload_table_info表中不存在,但对应的ORC表存在，需要人工处理,对应ORC表为:"
									+ dataLoadBean.getDb_name_orc() + "." + dataLoadBean.getTable_name_orc());
				}
				ArrayList<String> insert_table_info_list = metadataBean.getInsert_table_info_list();
				// 将orcale表信息写入到table_info中。
				conn_mysql = DataLoadPub.createMysqlConnSM4(dataLoadBean.getAuthUserMysql());
				conn_mysql.setAutoCommit(false);// 关闭事务自动提交
				for (int i = 0; i < insert_table_info_list.size(); i++) {
					String insert_sql = insert_table_info_list.get(i);
					PreparedStatement pstmt = conn_mysql.prepareStatement(insert_sql);
					// pubAPIs.writerLog("insert_sql:" + insert_sql);
					pstmt.executeUpdate();
				}
				conn_mysql.commit();
				conn_mysql.setAutoCommit(true);
				conn_mysql.close();
				tar_list = metadataBean.getSrc_list_curr();
			} else {
				tar_list = metadataBean.getSrc_list_bef(); // table_info表中的字段信息
			}
			int cur = metadataBean.getCur_size(); // sql文件中解析出来的字段个数
			int bef = metadataBean.getBef_size(); // table_info表中记录的字段个数
			if (cur > bef && bef != 0) {
				String warn_info = "警告:" + dataLoadBean.getTable_full_name_orc() + "表字段增加了! -> ";
				ArrayList<String[]> list = metadataBean.getSrc_list_curr(); // 从sql文件中解析出来的字段
				conn_tdh = DataLoadPub.createTDHConnSM4(dataLoadBean.getAuthUserTDH());
				Statement stmt_tdh_alt = conn_tdh.createStatement();
				conn_mysql = DataLoadPub.createMysqlConnSM4(dataLoadBean.getAuthUserMysql());
				conn_mysql.setAutoCommit(false);// 关闭事务自动提交
				for (int n = bef; n < cur; n++) {
					warn_info += list.get(n)[0] + " " + list.get(n)[1] + ";";
					String altsql = "ALTER TABLE " + dataLoadBean.getTable_full_name_orc() + " ADD COLUMNS("
							+ list.get(n)[0] + " " + list.get(n)[1] + ");";
					stmt_tdh_alt.execute(altsql);
					String insert_sql = (metadataBean.getInsert_table_info_list()).get(n);
					PreparedStatement addtmt = conn_mysql.prepareStatement(insert_sql);
					addtmt.executeUpdate();
				}
				stmt_tdh_alt.close();
				conn_mysql.commit();
				conn_mysql.setAutoCommit(true);
				conn_mysql.close();
				pubAPIs.writerLog(warn_info);
				DataLoadPub.write_warnInfo(dataLoadBean, "column", "3", warn_info);
				tar_list = metadataBean.getSrc_list_curr();
			}

			// 以下是拼接建TXT表的语句，建ORC表语句，insert into语句
			HashMap<String, String> comment_info = metadataBean.getComment_info();// 表注解与字段注解
			HashMap<String, HashMap<String, String>> columnMap = dataLoadBean.getColumnMap();
			HashMap<String, String> column_mapping_map = columnMap.get(dataLoadBean.getSrc_table_type());
			String txt_tab = dataLoadBean.getDb_name_txt() + "." + dataLoadBean.getTable_name_txt();
			String orc_tab = dataLoadBean.getDb_name_orc() + "." + dataLoadBean.getTable_name_orc();
			String createSQL_txt = "drop table " + txt_tab + ";\ncreate external table " + txt_tab + "(\n";
			String createSQL_orc = "drop table " + orc_tab + ";\ncreate table " + orc_tab + "(\n";
			String delete_sql = "delete from " + orc_tab + " tar where exists (select 1 from " + txt_tab
					+ " src where ";
			String insert_into_orc0 = "";
			String insert_into_orc1 = "insert into " + orc_tab;
			String insert_into_orc_bef = "(\n";
			String insert_into_orc_min = "";
			String insert_into_orc_min_merge_end = "";
			String merge_sql = "";

			String merge_sql_update = "\nwhen matched then update set \n";
			for (int i = 0; i < tar_list.size(); i++) {
				String columnsInfo[] = tar_list.get(i);
				String columnsName = columnsInfo[0].trim();
				String columnsType = columnsInfo[1].trim();
				String columnsTypeback = columnsType;
				if (load_condition.indexOf(columnsName) == -1) {
					merge_sql_update += "  tar." + columnsName + "=src." + columnsName + ",\n";
				}
				int a = columnsType.indexOf("(");
				if (a != -1) {
					if (columnsType.indexOf("number") != -1) {
						if (columnsType.indexOf(",") != -1) {
							int b = columnsType.indexOf(",");
							int c = columnsType.indexOf(")");
							int d = Integer.parseInt(columnsType.substring(a + 1, b))
									+ Integer.parseInt(columnsType.substring(b + 1, c));
							d = d > 38 ? 38 : d;
							columnsType = "decimal" + "(" + d + "," + columnsType.substring(b + 1, c) + ")";
						} else {
							columnsType = columnsType.substring(0, a);
							columnsType = column_mapping_map.get(columnsType);
						}
					} else {
						columnsType = columnsType.substring(0, a);
						columnsType = column_mapping_map.get(columnsType);
						if (columnsType == null)
							columnsType = columnsTypeback;
					}
				} else {
					columnsType = column_mapping_map.get(columnsType);
					if (columnsType == null)
						columnsType = columnsTypeback;
				}
				String columnsComment = comment_info.get(src_table_name + "." + columnsInfo[0].trim());
				if (columnsComment == null)
					columnsComment = "";
				createSQL_txt += "  " + columnsName + " " + columnsType
						+ ("".equals(columnsComment.trim()) ? "" : (" comment " + columnsComment)) + ",\n";
				createSQL_orc += "  " + columnsName + " " + columnsType
						+ ("".equals(columnsComment.trim()) ? "" : (" comment " + columnsComment)) + ",\n";
				insert_into_orc_bef += "  " + columnsName + ",\n";
				insert_into_orc_min += "  src." + columnsName + ",\n";
			}
			merge_sql_update += "  tar.etl_date_tdh='" + etl_date + "' ";
			insert_into_orc_bef += "  bucket_id_tdh,\n  system_id_tdh,\n  etl_date_tdh\n)";
			insert_into_orc_min_merge_end = insert_into_orc_min
					+ "  src.bucket_id_tdh,\n  src.system_id_tdh,\n  src.etl_date_tdh \n";

			if (!usr_def_partition.equals("") && usr_def_partition != null) {
				insert_into_orc_min += "  uniq() as bucket_id_tdh,\n  '" + dataLoadBean.getSrc_sys_id()
						+ "' as system_id_tdh,\n  " + usr_def_partition + " as etl_date_tdh \n"; // to_char(to_date(to_char("+batch_day+")),'yyyymmdd')
			} else {
				insert_into_orc_min += "  uniq() as bucket_id_tdh,\n  '" + dataLoadBean.getSrc_sys_id()
						+ "' as system_id_tdh,\n  '" + etl_date + "' as etl_date_tdh \n";
			}

			// 生成建表语句
			String tableComment = comment_info.get(src_table_name);
			if (comment_info.get(src_table_name) == null)
				tableComment = "\'\'";
			tableComment = "comment " + tableComment + "\n";
			if ("1".equals(dataLoadBean.getCol_map_type())) {
				if ("add".equals(load_type)) {// 如果是增量则不建按天分区表
					if ((dataLoadBean.getUsr_def_partition()).indexOf("to_char(") != -1) {
						createSQL_orc += " bucket_id_tdh string,\n  system_id_tdh string \n)" + tableComment
								+ " partitioned by range(etl_date_tdh string) \nclustered by(bucket_id_tdh) into "
								+ "${bucket_num} buckets stored as orc \ntblproperties('transactional'='true');";
					} else {
						createSQL_orc += " bucket_id_tdh string,\n  system_id_tdh string \n,etl_date_tdh string\n)"
								+ tableComment + " partitioned by range(" + dataLoadBean.getUsr_def_partition()
								+ ") \nclustered by(bucket_id_tdh) into "
								+ "${bucket_num} buckets stored as orc \ntblproperties('transactional'='true');";
					}

				} else {// 如果是全量或merge则不建分区表
					int bucket_num = (int) (txtFileSize / bucket_size_default) + 1;
					createSQL_orc += " bucket_id_tdh string,\n system_id_tdh string,\n   etl_date_tdh string)\n"
							+ tableComment + "clustered by(bucket_id_tdh) into " + bucket_num
							+ " buckets stored as orc \ntblproperties('transactional'='true')";
				}
			} else {
				createSQL_orc += ")" + tableComment;
				createSQL_orc = createSQL_orc.replace(",\n)", "\n)");
			}
			createSQL_txt += ")" + tableComment;
			createSQL_txt = createSQL_txt.replace(",\n)", "\n)");
			createSQL_txt += "ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' \n"
					+ "WITH SERDEPROPERTIES('input.delimited'='" + dataLoadBean.getDelimited() + "') \n" + "location '"
					+ dataLoadBean.getDataFile_hist_dir() + "/data' \n"
					+ "tblproperties(\n'serialization.encoding'='utf-8');";
			// 更新临时TXT表
			conn_tdh = DataLoadPub.createTDHConnSM4(dataLoadBean.getAuthUserTDH());
			Statement stmt20 = conn_tdh.createStatement();
			pubAPIs.writerLog("createSQL_txt:" + createSQL_txt);
			stmt20.addBatch(createSQL_txt);
			stmt20.executeBatch();
			stmt20.clearBatch();
			stmt20.close();
			conn_tdh.close();
			// 生成更新语句

			String merge_sql_bef = "merge into " + dataLoadBean.getTable_full_name_orc() + " tar using ";
			merge_sql_bef += "\n(select\n" + insert_into_orc_min.replace(" src", " src0") + "from "
					+ dataLoadBean.getTable_full_name_txt() + " src0) src on ";
			String load_conditions[] = load_condition.split(",");
			for (int i = 0; i < load_conditions.length; i++) {
				if (!"".equals(load_conditions[i])) {
					merge_sql_bef += (i == 0 ? "tar." : " and tar.") + load_conditions[i] + "=src."
							+ load_conditions[i];
					delete_sql += (i == 0 ? "tar." : " and tar.") + load_conditions[i] + "=src." + load_conditions[i];
				}
			}
			merge_sql = merge_sql_bef + merge_sql_update + "\nwhen not matched then insert " + insert_into_orc_bef
					+ " values (\n" + insert_into_orc_min_merge_end + ")\nwhere tar.etl_date_tdh <= '"
					+ dataLoadBean.getEtl_date() + "';";
			// pubAPIs.writerLog("merge_sql:" + merge_sql);

			delete_sql += ");";

			insert_into_orc1 += insert_into_orc_bef + "\n select \n" + insert_into_orc_min + "from " + txt_tab
					+ " src ";

			if (existsTarTab) {// 如果ORC表存在
				if ("merge".equals(load_type)) {
					conn_tdh = DataLoadPub.createTDHConnSM4(dataLoadBean.getAuthUserTDH());
					Statement stmt25 = conn_tdh.createStatement();
					pubAPIs.writerLog("merge_sql:" + merge_sql);
					stmt25.execute(merge_sql);
					int tar_mun = stmt25.getUpdateCount();
					if (tar_mun != src_mun) {
						String warn_info = "警告:" + dataLoadBean.getTable_full_name_orc() + "表字段增加了! -> ";
						pubAPIs.writerLog(warn_info);
						DataLoadPub.write_warnInfo(dataLoadBean, "column", "3", warn_info);
					}
					dataLoadBean.setJob_tar_count(tar_mun);
					stmt25.close();
					conn_tdh.close();
				} else if ("update".equals(load_type)) {
					conn_tdh = DataLoadPub.createTDHConnSM4(dataLoadBean.getAuthUserTDH());
					Statement stmt22 = conn_tdh.createStatement();
					pubAPIs.writerLog("delete_sql:" + delete_sql);
					stmt22.addBatch(delete_sql);
					stmt22.executeBatch();
					stmt22.clearBatch();
					stmt22.close();
					Statement stmt23 = conn_tdh.createStatement();
					pubAPIs.writerLog("insert_into_orc1:" + insert_into_orc1);
					stmt23.execute(insert_into_orc1);
					int tar_mun = stmt23.getUpdateCount();
					if (tar_mun != src_mun) {
						throw new PubException("入库条数与源系统提供条数不一致!分别为:" + tar_mun + " " + src_mun);
					}
					dataLoadBean.setJob_tar_count(tar_mun);
					stmt23.close();
					conn_tdh.close();
				} else {
					if ("full".equals(load_type)) {
						insert_into_orc0 = "truncate table " + orc_tab + ";\n";
					} else if ("add".equals(load_type)) {
						if (etl_date.compareTo(max_partition_end) >= 0) {
							insert_into_orc0 = "alter table " + orc_tab + " add if not exists partition partition_"
									+ etl_date + " values less than ('" + next_partition_date + "');\n";
						}
						if ((dataLoadBean.getUsr_def_partition()).indexOf("to_char(") != -1) {
							if(!dataLoadBean.getDef_etl_date().equals("") && dataLoadBean.getDef_etl_date() != null) {
								int a = 2;
								int b = dataLoadBean.getDef_etl_date().length();
								String c = dataLoadBean.getDef_etl_date().substring(a,b).trim();
								int d = Integer.parseInt(c)-1;
								Calendar cd = Calendar.getInstance();
								Date date = null;
								date = sdf1.parse(etl_date);
								cd.setTime(date);
								int day = cd.get(cd.DATE);
							    cd.set(cd.DATE,day-d);
								String redef_etl_date = sdf1.format(cd.getTime());
							    insert_into_orc0 += "delete from " + orc_tab + " where etl_date_tdh = '" + redef_etl_date + "';";
							}else {
							    insert_into_orc0 += "delete from " + orc_tab + " where etl_date_tdh = '" + etl_date + "';";
							}
						} else {
							if(!dataLoadBean.getDef_etl_date().equals("") && dataLoadBean.getDef_etl_date() != null) {
								int a = 2;
								int b = dataLoadBean.getDef_etl_date().length();
								String c = dataLoadBean.getDef_etl_date().substring(a,b).trim();
								int d = Integer.parseInt(c)-1;
								Calendar cd = Calendar.getInstance();
								Date date = null;
								date = sdf1.parse(etl_date);
								cd.setTime(date);
								int day = cd.get(cd.DATE);
							    cd.set(cd.DATE,day-d);
								String redef_etl_date = sdf1.format(cd.getTime());
							    insert_into_orc0 += "delete from " + orc_tab + " where "
									+ dataLoadBean.getUsr_def_partition() + " = '" + redef_etl_date + "';";
							}else {
								insert_into_orc0 += "delete from " + orc_tab + " where "
								+ dataLoadBean.getUsr_def_partition() + " = '" + etl_date + "';";
								
							}
						}
					}
					conn_tdh = DataLoadPub.createTDHConnSM4(dataLoadBean.getAuthUserTDH());
					Statement stmt22 = conn_tdh.createStatement();
					pubAPIs.writerLog("insert_into_orc0:" + insert_into_orc0);
					stmt22.addBatch(insert_into_orc0);
					stmt22.executeBatch();
					stmt22.clearBatch();
					stmt22.close();
					Statement stmt23 = conn_tdh.createStatement();
					pubAPIs.writerLog("insert_into_orc1:" + insert_into_orc1);
					stmt23.execute(insert_into_orc1);
					int tar_mun = stmt23.getUpdateCount();
					if (tar_mun != src_mun) {
						throw new PubException("入库条数与源系统提供条数不一致!分别为:" + tar_mun + " " + src_mun);
					}
					dataLoadBean.setJob_tar_count(tar_mun);
					stmt23.close();
					conn_tdh.close();
				}
			} else {// 如果ORC表不存在
				conn_tdh = DataLoadPub.createTDHConnSM4(dataLoadBean.getAuthUserTDH());
				if ("add".equals(load_type) && !dataLoadBean.getUsr_def_partition().equals("")
						&& dataLoadBean.getUsr_def_partition() != null) {
					Statement stmt_tdh2 = conn_tdh.createStatement();
					String eveDay_count = "SELECT " + dataLoadBean.getUsr_def_partition()
							+ " AS elt_date , count(*) AS eveDay_count FROM " + dataLoadBean.getTable_full_name_txt()
							+ " GROUP BY " + dataLoadBean.getUsr_def_partition() + " ORDER BY "
							+ dataLoadBean.getUsr_def_partition() + " ;";//
					pubAPIs.writerLog("eveDay_count:" + eveDay_count);
					ResultSet rs1 = stmt_tdh2.executeQuery(eveDay_count);
					List<String[]> txt_info_list = new ArrayList<String[]>();
					int txt_tab_days = 0;// TXT表的累计天数
					String txt_first_day = "";
					String txt_last_day = "";
					long txt_tab_counts = 0;// TXT表的累计条数
					while (rs1.next()) {
						String elt_date = rs1.getString("elt_date");
						int counts = rs1.getInt("eveDay_count");
						String[] temp = new String[2];
						temp[0] = elt_date;
						temp[1] = Integer.toString(counts);
						txt_info_list.add(temp);
						if (txt_tab_days == 0) {
							if (elt_date == null)
								throw new PubException("分区字段存在空值:" + dataLoadBean.getUsr_def_partition());
							txt_first_day = elt_date;
						}
						txt_tab_days++;
						txt_tab_counts += counts;
						txt_last_day = elt_date;
					}
					rs1.close();
					stmt_tdh2.close();
					if (txt_first_day.equals(""))
						txt_first_day = dataLoadBean.getEtl_date();// 表中没有数据
					Calendar c5 = Calendar.getInstance();
					c5.setTime(sdf1.parse(txt_first_day));
					c5.add(Calendar.DATE, -1);
					txt_first_day = sdf1.format(c5.getTime());

					int bucket_num = 1;
					double day_size = txtFileSize * 1.0 / txt_tab_days * 1.0;// 在orc表中平均每天数据量大小,单位M
					double parition_size = day_size * parition_days_default; // 按天进行分区，计算出来的分区标准
					pubAPIs.writerLog("parition_days_default---->" + parition_days_default);
					double parition_size_max = parition_size;// 以49天数据作为分区标准
					if (parition_size > (bucket_number_max * bucket_size_default)) {// 如果计算出来的49天数据量大于设定的最大默认数据量
						parition_size_max = bucket_number_max * bucket_size_default;// 当49天的数据量大于默认值时，用默认值作为分区的标准
					}
					bucket_num = (int) (parition_size_max / bucket_size_default) + 1;
					createSQL_orc = createSQL_orc.replace("${bucket_num}", bucket_num + "");
					Statement stmt21 = conn_tdh.createStatement();
					pubAPIs.writerLog("createSQL_orc:" + createSQL_orc);
					stmt21.execute(createSQL_orc);
					String alter_addpartition_sql = "alter table " + orc_tab + " add if not exists partition partition_"
							+ txt_first_day + " values less than ('" + txt_first_day + "');\n";
					pubAPIs.writerLog("alter_addpartition_sql:" + alter_addpartition_sql);
					stmt21.execute(alter_addpartition_sql);
					stmt21.executeBatch();
					stmt21.clearBatch();
					stmt21.close();
					conn_tdh.close();
					double day_size_curr = 0.00;
					last_partition_ENDdate = txt_first_day;
					int tar_mun = 0;

					for (int i = 0; i < txt_info_list.size(); i++) {
						String[] temp = txt_info_list.get(i);
						String etl_date_name = temp[0];
						int etl_date_count = Integer.parseInt(temp[1]);
						if (day_size_curr == 0) {
							txt_first_day = etl_date_name;
						}
						day_size_curr += (txtFileSize * etl_date_count) * 1.0 / txt_tab_counts * 1.0;// 当前累计大小，单位为M
						if (etl_date_name == txt_last_day || day_size_curr >= parition_size_max) {
							conn_tdh = DataLoadPub.createTDHConnSM4(dataLoadBean.getAuthUserTDH());
							if (!etl_date_name.equals(txt_last_day)) {
								next_partition_date = etl_date_name;
							} else {// 当最后一轮
								Calendar c1 = Calendar.getInstance();
								Calendar c2 = Calendar.getInstance();
								c1.setTime(sdf1.parse(txt_first_day));
								c2.setTime(sdf1.parse(txt_last_day));
								long time1 = c1.getTimeInMillis();
								long time2 = c2.getTimeInMillis();
								long between_days = (time2 - time1) / (1000 * 3600 * 24);
								day_num = Integer.parseInt(String.valueOf(between_days));// 得到上个分期的天数
								int parition_day_num = day_num * (int) (parition_size_max / day_size_curr) + 1;
								c1.add(Calendar.DATE, parition_day_num);
								next_partition_date = sdf1.format(c1.getTime());
							}
							Statement stmt33 = conn_tdh.createStatement();

							String alter_addpartition_sql1 = "alter table " + orc_tab
									+ " add if not exists partition partition_" + txt_first_day + " values less than ('"
									+ next_partition_date + "');\n";
							pubAPIs.writerLog("alter_addpartition_sql1:" + alter_addpartition_sql1);
							stmt33.execute(alter_addpartition_sql1);
							txt_first_day = sdf1.format(c5.getTime());
							String insert_into_orc = insert_into_orc1 + " WHERE " + dataLoadBean.getUsr_def_partition()
									+ ">='" + last_partition_ENDdate + "' AND " + dataLoadBean.getUsr_def_partition()
									+ "<'" + next_partition_date + "'";
							pubAPIs.writerLog(dataLoadBean.getTable_full_name_orc() + "开始插入分区数据 ");
							pubAPIs.writerLog("insert_into_orc:" + insert_into_orc);
							stmt33.execute(insert_into_orc);
							int tmp_mun = stmt33.getUpdateCount();
							tar_mun += tmp_mun;
							stmt33.close();
							pubAPIs.writerLog("\n" + dataLoadBean.getTable_full_name_orc() + "分区数据插入成功:" + tmp_mun);
							last_partition_ENDdate = next_partition_date;// 记录上一个分区最后一天的日期
							day_size_curr = 0;

							conn_tdh.close();
						}
					}
					dataLoadBean.setJob_tar_count(tar_mun);
					if (tar_mun != src_mun) {
						throw new PubException("入库条数与源系统提供条数不一致!分别为:" + tar_mun + " " + src_mun);
					}

				} else {
					Statement stmt21 = conn_tdh.createStatement();
					pubAPIs.writerLog("createSQL_orc:" + createSQL_orc);
					stmt21.addBatch(createSQL_orc);
					stmt21.executeBatch();
					stmt21.clearBatch();
					stmt21.close();

					Statement stmt26 = conn_tdh.createStatement();
					pubAPIs.writerLog("insert_into_orc1:" + insert_into_orc1);
					pubAPIs.writerLog("开始插入数据");
					stmt26.execute(insert_into_orc1);
					int tar_mun = stmt26.getUpdateCount();
					pubAPIs.writerLog("数据插入成功:" + tar_mun);
					dataLoadBean.setJob_tar_count(tar_mun);
					if (tar_mun != src_mun) {
						throw new PubException("入库条数与源系统提供条数不一致!分别为:" + tar_mun + " " + src_mun);
					}
					stmt26.close();
					conn_tdh.close();
				}
			}
			long end1 = new Date().getTime();
			tatol1 = (int) ((end1 - start1) * 1 / 1000);
			dataLoadBean.setJob_used_time((int) tatol1);
			DataLoadPub.write_jobLog(dataLoadBean, "2", "作业结束");
		} catch (Exception e) {
			String eStr = pubAPIs.getException(e);
			pubAPIs.writerLog(eStr);
			DataLoadPub.threadLock(thread_id, "minus", "", 0);
			DataLoadPub.write_jobLog(dataLoadBean, "1", eStr);
		} finally {
			try {
				PubAPIs.getFileSystem(dataLoadBean.getConf_hdfs(), dataLoadBean.getLogin_kerberos(), "dataHandle",
						"close", fs);
				if (conn_mysql != null && !conn_mysql.isClosed()) {
					conn_mysql.close();
				}
				if (conn_tdh != null && !conn_tdh.isClosed()) {
					conn_tdh.close();
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
			}
		}

		return result;
	}
}
