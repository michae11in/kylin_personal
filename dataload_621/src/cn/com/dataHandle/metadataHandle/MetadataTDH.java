package cn.com.dataHandle.metadataHandle;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;

import cn.com.dataHandle.EtlDataPub;
import cn.com.dataHandle.TransferDataPub;
import cn.com.dataHandle.bean.MetadataTDHBean;
import cn.com.dataHandle.bean.TranfersDataBean;
import cn.com.pub.PubAPIs;

public class MetadataTDH {
	private PubAPIs pubAPIs = new PubAPIs("dataHandle");
	private Connection conn_tdh = null;
	private String log_file_dir = "";

	public MetadataTDHBean analysTdhMetadataFromDB(TranfersDataBean tranfersDataBean, String paritionsRange)
			throws Exception {
		MetadataTDHBean metadataBean = new MetadataTDHBean();
		String db_name = tranfersDataBean.getDb_name();
		String tab_name = tranfersDataBean.getTab_name();
		String tab_full_name = tranfersDataBean.getTab_full_name();
		log_file_dir = tranfersDataBean.getLog_file_dir();
		PubAPIs.setLogsPath(log_file_dir + "TranfersDataExp" + PubAPIs.getDirSign());
		PubAPIs.setErrorLogsPath(log_file_dir + "TranfersDataExpError" + PubAPIs.getDirSign());
		this.pubAPIs.setClassName(tab_full_name);
		pubAPIs.writerLog("Run analysTdhMetadataFromDB:" + tab_full_name);
		try {
			String createTableSQL = "";
			String createTmpTabSQL = "";
			String dropTmpTabSQL = "";
			String tab_location = "";
			String columnStr = "";
			String inputFormat = "no_orc";
			String transactional = "false";
			String partition_insert_head = "";
			String isPartition = "false";
			String partition_columns = "";
			String isRangePartition = "false";
			String temp_database = tranfersDataBean.getTemp_database();
			String temp_tab_name = tab_full_name.replace(".", "_");
			String tmp_tab_full_name = temp_database + "." + temp_tab_name;
			List<String[]> partition_list = new ArrayList<String[]>();
			HashMap<String, String> partition_whereStr = new HashMap<String, String>();
			HashMap<String, String> partition_dropPartition = new HashMap<String, String>();
			HashMap<String, String> partition_addPartition = new HashMap<String, String>();
			HashMap<String, String> partition_trunPartition = new HashMap<String, String>();
			ArrayList<String[]> tar_columns_list = new ArrayList<String[]>();//目标表的字段以及字段类型
		
			conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
			Statement stmt = conn_tdh.createStatement();
			String sql = "SHOW CREATE TABLE " + tab_full_name;
			ResultSet rs = stmt.executeQuery(sql);
			StringBuffer resultSql = new StringBuffer();
			while (rs.next()) {
				String tmp = rs.getString(1).replace("\n", "");
				resultSql.append(tmp);
				resultSql.append("\001");
			}
			rs.close();
			stmt.close();
			conn_tdh.close();

			int column_sign = 0;
			int column_sign1 = 0;
			int partitioned_sign = 0;
			int serde_sign = 0;
			String serde_type = "";
			createTableSQL = "drop table " + tab_full_name + ";\n";
			createTmpTabSQL = "drop table " + tmp_tab_full_name + ";\n";
			String[] resultSqls = resultSql.toString().split("\001");
			for (int i = 0; i < resultSqls.length; i++) {
				String tmp = resultSqls[i];
				// pubAPIs.writerLog(tmp);
				if (tmp.indexOf("hdfs://nameservice") == -1 && (!"LOCATION".equals(tmp.trim()))) {
					//去除掉範圍分區表的comment內容
					if(tmp.indexOf(") (") !=-1 && tmp.indexOf("COMMENT") !=-1) {
						tmp=tmp.substring(0,tmp.indexOf("COMMENT")-1)+" ) (";
						createTableSQL += tmp + "\n";
					//去除掉單只分區表的非法ORACLE內容
					}else if(tmp.indexOf(",ORACLE)") !=-1 ){
						tmp=tmp.replace(",ORACLE", "");
						createTableSQL += tmp + "\n";
					}else {
						createTableSQL += tmp + "\n";
					}
				} else if (tmp.indexOf("hdfs://nameservice") != -1) {
					tab_location = tmp.replace("hdfs://nameservice", "").replace("'", "");
					tab_location = tab_location.substring(tab_location.indexOf("/"), tab_location.length()) + "/";
				}
				if (column_sign == 0) {
					columnStr += tmp.trim().split(" ")[0] + ",";//字段和字段类型写入
					tmp = tmp.replaceAll("\\s+", " ").trim().toLowerCase();
					if(tmp.indexOf("create table") == -1&&!")".equals(tmp))  {
						String[] tarcolums=tmp.trim().split(" ");
						String[] columns_info_tar = new String[] { tarcolums[0], tarcolums[1] };
						tar_columns_list.add(columns_info_tar);
					}
				}
				if ("true".equals(isPartition) && "false".equals(isRangePartition) && column_sign != 0
						&& column_sign1 >= 0) {
					columnStr += tmp.trim().split(" ")[0] + ",";
					column_sign1++;
					if (tmp.indexOf(")") != -1) {
						column_sign1 = -1;
					}
				}
				String tmp1 = tmp.trim();
				if (")".equals(tmp1)) {
					column_sign = 1;
				}
				if (serde_sign == 1) {
					serde_type = tmp1.trim().replace("'", "");
					serde_sign = 2;
				}
				if (tmp.indexOf("ROW FORMAT SERDE") != -1) {
					serde_sign = 1;
				}
				if (serde_sign == 0) {
					createTmpTabSQL += tmp + "\n";
				}
				if (column_sign == 1) {
					if (tmp1.indexOf("'transactional'='true'") != -1) {
						transactional = "true";
					}
					if (tmp1.indexOf("PARTITIONED BY") != -1) {
						isPartition = "true";
					}
					if (partitioned_sign == 1) {
						String tmp2 = tmp1.trim();
						if (tmp2.indexOf(")") != -1) {
							partitioned_sign = 0;
						}
						tmp2 = tmp2.replace(")", "").replace("(", "");
						tmp2 = tmp2.trim();
						partition_columns += tmp2.trim();
						String[] tmp3 = tmp2.split(",");
						for (int n = 0; n < tmp3.length; n++) {
							String tmp5 = tmp3[n];
							partition_insert_head += tmp5.split(" ")[0] + ",";
						}
					}
					if (tmp1.indexOf("PARTITIONED BY") != -1) {
						partitioned_sign = 1;
					} // 放在后面才不会导致partition_columns不正确

					if (tmp1.indexOf("PARTITIONED BY RANGE") != -1) {
						isRangePartition = "true";
					}
					if (tmp1.indexOf("PARTITION ") != -1) {
						String PARTITION = tmp1.substring(tmp1.indexOf("PARTITION") + 9, tmp1.indexOf(" VALUES"));
						String PARTITION_VALUE = tmp1.substring(tmp1.indexOf("("), tmp1.indexOf(")") + 1);
						PARTITION = PARTITION.trim();
						PARTITION_VALUE = PARTITION_VALUE.trim();
						String[] PARTITION_NAME_VALUE = { PARTITION, PARTITION_VALUE };
						if ("all".equals(paritionsRange) || paritionsRange.indexOf(PARTITION) != -1) {
							partition_addPartition.put(PARTITION,
									"ALTER TABLE " + tab_full_name + " ADD IF NOT EXISTS PARTITION " + PARTITION
											+ " VALUES LESS THAN " + PARTITION_VALUE + ";");
							partition_list.add(PARTITION_NAME_VALUE);
							partition_dropPartition.put(PARTITION,
									"ALTER TABLE " + tab_full_name + " DROP PARTITION " + PARTITION + ";");
							partition_trunPartition.put(PARTITION,
									"truncate table " + tab_full_name + " partition " + PARTITION + ";");
						}
					}
				}
			}
			conn_tdh = TransferDataPub.createTDHConnSM4(tranfersDataBean.getTdh_login_ldap());
			if ("false".equals(isRangePartition) && "true".equals(isPartition)) {
				Statement stmt1 = conn_tdh.createStatement();
				String sql1 = "select t.partition_name,t.partition_value from system.partitions_v t "
						+ " where t.database_name ='" + db_name + "' and t.table_name='" + tab_name
						+ "' order by t.partition_name";
				ResultSet rs2 = stmt1.executeQuery(sql1);

				while (rs2.next()) {
					String PARTITION_src = rs2.getString("partition_name");
					String PARTITION_VALUE = rs2.getString("partition_value");
					String[] PARTITION_NAME_VALUE = { PARTITION_src, PARTITION_VALUE };
					String PARTITION = "(" + PARTITION_src.replace("=", "=\"") + "\")";
					PARTITION = PARTITION.replace("/", "\",");
					String PARTITION_temp = PARTITION.replace("\"", "").replace("'", "").replace("(", "").replace(")",
							"");
					if ("all".equals(paritionsRange) || paritionsRange.indexOf(PARTITION_temp) != -1) {
						partition_list.add(PARTITION_NAME_VALUE);
						partition_addPartition.put(PARTITION_src,
								"ALTER TABLE " + tab_full_name + " ADD IF NOT EXISTS PARTITION " + PARTITION + ";");
						partition_dropPartition.put(PARTITION_src,
								"ALTER TABLE " + tab_full_name + " DROP PARTITION " + PARTITION + ";");
						partition_trunPartition.put(PARTITION_src,
								"truncate table " + tab_full_name + " partition " + PARTITION + ";");
					}
				}
				rs2.close();
				stmt1.close();
			}
			conn_tdh.close();
			// -----------元数据解析结束end
			if ("true".equals(isRangePartition)) {// 通过partition_list拼接where字符串
				String[] partition_column = partition_columns.split(",");
				String[] partition_value_bef = null;
				HashMap<String, String> maxValueMap = new HashMap<String, String>();
				for (int i = 0; i < partition_list.size(); i++) {
					String partition_name = partition_list.get(i)[0];
					String partition_values = partition_list.get(i)[1];
					partition_values = partition_values.replace(" ", "");
					partition_values = partition_values.replace("(", "");
					partition_values = partition_values.replace(")", "");
					String[] partition_value = partition_values.split(",");
					String whereStr = "(";
					for (int j = 0; j < partition_column.length; j++) {
						String partition_column_name = partition_column[j];
						String partition_column_value = partition_value[j];
						String partition_column_bef_value = partition_value_bef != null ? partition_value_bef[j] : "";
						String partition_value_tmp = "";
						if (partition_value_bef == null || "MINVALUE".equals(partition_column_value)) {
							partition_value_tmp = partition_column_name + "<" + partition_column_value + " and ";
						} else if ("MAXVALUE".equals(partition_column_value)) {
							partition_value_tmp = partition_column_name + ">=" + partition_column_bef_value + " and ";
						} else {
							partition_value_tmp = partition_column_name + ">=" + partition_column_bef_value + " and "
									+ partition_column_name + "<" + partition_column_value + " and ";
						}
						String partition_value_bef_tmp = maxValueMap.get(partition_column_name);
						if (partition_value_bef_tmp != null
								&& partition_column_value.equals(partition_column_bef_value)) {
							whereStr += partition_value_bef_tmp;
						} else {
							whereStr += partition_value_tmp;
							maxValueMap.put(partition_column_name, partition_value_tmp);
						}
					}
					whereStr += ")";
					whereStr = whereStr.replace(" and )", ")");
					whereStr = whereStr.replace("'", "\"");
					partition_value_bef = partition_value;
					partition_whereStr.put(partition_name, whereStr);
				}
			} else {
				for (int i = 0; i < partition_list.size(); i++) {
					String[] partitions = partition_list.get(i);
					String partition_name = partitions[0];
					String whereStr = "(" + partition_name + "\")";
					whereStr = whereStr.replace("=", "=\"");
					whereStr = whereStr.replace("/", "\" and ");
					partition_whereStr.put(partition_name, whereStr);
				}
			}

			columnStr = columnStr.toLowerCase().replace("create,", "").replace("),", "");
			columnStr = columnStr.substring(0, columnStr.length() - 1);
			createTableSQL += ";";
			dropTmpTabSQL = "drop table " + tmp_tab_full_name + ";";
			if (serde_type.indexOf("LazySimpleSerDe") != -1) {
				createTmpTabSQL = createTableSQL.replace(tab_full_name, tmp_tab_full_name);
			} else {
				createTmpTabSQL = createTmpTabSQL.replace(tab_full_name, tmp_tab_full_name) + " STORED AS ORC;";
			}
			if (partition_insert_head != null && partition_insert_head.length() >= 1) {
				partition_insert_head = " PARTITION ("
						+ partition_insert_head.substring(0, partition_insert_head.length() - 1) + ") ";
				partition_insert_head = partition_insert_head.replace("'", "\"");
			}

			if ((serde_type.indexOf("OrcSerde") != -1 || serde_type.indexOf("LazySimpleSerDe") != -1)
					&& "false".equals(transactional)) {
				inputFormat = "orc";
			}
			metadataBean.setPartition_insert_head(partition_insert_head);
			metadataBean.setPartition_addPartition(partition_addPartition);
			metadataBean.setColumnStr(columnStr);
			metadataBean.setCreateTableSQL(createTableSQL);
			metadataBean.setCreateTmpTabSQL(createTmpTabSQL);
			metadataBean.setDropTmpTabSQL(dropTmpTabSQL);
			metadataBean.setPartition_dropPartition(partition_dropPartition);
			metadataBean.setParitionsRange(paritionsRange);
			metadataBean.setTab_location(tab_location);
			metadataBean.setInputFormat(inputFormat);
			metadataBean.setTransactional(transactional);
			metadataBean.setIsPartition(isPartition);
			metadataBean.setIsRangePartition(isRangePartition);
			metadataBean.setPartition_columns(partition_columns);
			metadataBean.setPartition_list(partition_list);
			metadataBean.setPartition_whereStr(partition_whereStr);
			metadataBean.setTmp_tab_full_name(tmp_tab_full_name.toLowerCase());
			metadataBean.setTemp_tab_name(temp_tab_name.toLowerCase());
			metadataBean.setPartition_trunPartition(partition_trunPartition);
			metadataBean.setTemp_db_name(tranfersDataBean.getTemp_database().toLowerCase());
			metadataBean.setTab_full_name(tab_full_name.toLowerCase());
			metadataBean.setSerde_type(serde_type);
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
			if (conn_tdh != null && !conn_tdh.isClosed()) {
				conn_tdh.close();
			}
		}
		return metadataBean;
	}

	public MetadataTDHBean analysTdhMetadataFromTXT(TranfersDataBean tranfersDataBean, String paritionsRange)
			throws Exception {
		MetadataTDHBean metadataBean = new MetadataTDHBean();
		String tab_full_name = tranfersDataBean.getTab_full_name();
		log_file_dir = tranfersDataBean.getLog_file_dir();
		PubAPIs.setLogsPath(log_file_dir + "TranfersDataExp" + PubAPIs.getDirSign());
		PubAPIs.setErrorLogsPath(log_file_dir + "TranfersDataExpError" + PubAPIs.getDirSign());
		this.pubAPIs.setClassName(tab_full_name);
		pubAPIs.writerLog("Run analysTdhMetadataFromTXT:" + tab_full_name);
		try {
			String createTableSQL = "";
			String createTmpTabSQL = "";
			String dropTmpTabSQL = "";
			String tab_location = "";
			String columnStr = "";
			String inputFormat = "no_orc";
			String transactional = "false";
			String partition_insert_head = "";
			String isPartition = "false";
			String partition_columns = "";
			String isRangePartition = "false";
			String temp_database = tranfersDataBean.getTemp_database();
			String temp_tab_name = tab_full_name.replace(".", "_");
			String tmp_tab_full_name = temp_database + "." + temp_tab_name;
			List<String[]> partition_list = new ArrayList<String[]>();
			HashMap<String, String> partition_whereStr = new HashMap<String, String>();
			HashMap<String, String> partition_dropPartition = new HashMap<String, String>();
			HashMap<String, String> partition_addPartition = new HashMap<String, String>();
			HashMap<String, String> partition_trunPartition = new HashMap<String, String>();
			ArrayList<String[]> src_columns_list = new ArrayList<String[]>();//源表的字段以及字段类型
			StringBuffer resultSql = new StringBuffer();
			String tabInfo_file_path = tranfersDataBean.getTabInfo_file_path();
			String tabInfo_infor = "";
			File tabInfo_file = new File(tabInfo_file_path);
			if (tabInfo_file.exists()) {
				tabInfo_infor = FileUtils.readFileToString(tabInfo_file);
			} else {
				metadataBean.setResult(-1);
				return metadataBean;
			}
			if (!"".equals(tabInfo_infor)) {
				resultSql.append(tabInfo_infor.replace("\n", "\001"));
			} else {
				metadataBean.setResult(-1);
				return metadataBean;
			}

			int column_sign = 0;
			int column_sign1 = 0;
			int partitioned_sign = 0;
			int serde_sign = 0;
			String serde_type = "";
			createTableSQL = "drop table " + tab_full_name + ";\n";
			createTmpTabSQL = "drop table " + tmp_tab_full_name + ";\n";
			String[] resultSqls = resultSql.toString().split("\001");
			for (int i = 0; i < resultSqls.length; i++) {
				String tmp = resultSqls[i];
				if (tmp.indexOf("drop table ") == 0) {
					continue;
				}
				if (tmp.indexOf("hdfs://nameservice") == -1 && (!"LOCATION".equals(tmp.trim()))) {
					createTableSQL += tmp + "\n";
				} else if (tmp.indexOf("hdfs://nameservice") != -1) {
					tab_location = tmp.replace("hdfs://nameservice", "").replace("'", "");
					tab_location = tab_location.substring(tab_location.indexOf("/"), tab_location.length()) + "/";
				}
				if (column_sign == 0) {
					columnStr += tmp.trim().split(" ")[0] + ",";
					tmp = tmp.replaceAll("\\s+", " ").trim().toLowerCase();
					if(tmp.indexOf("create table") == -1&&!")".equals(tmp))  {
						String[] srccolums=tmp.trim().split(" ");
						String[] columns_info_src = new String[] { srccolums[0], srccolums[1] };
						src_columns_list.add(columns_info_src);
					}
					
				}
				if ("true".equals(isPartition) && "false".equals(isRangePartition) && column_sign != 0
						&& column_sign1 >= 0) {
					columnStr += tmp.trim().split(" ")[0] + ",";
					column_sign1++;
					if (tmp.indexOf(")") != -1) {
						column_sign1 = -1;
					}
				}
				String tmp1 = tmp.trim();
				if (")".equals(tmp1)) {
					column_sign = 1;
				}
				if (serde_sign == 1) {
					serde_type = tmp1.trim().replace("'", "");
					serde_sign = 2;
				}
				if (tmp.indexOf("ROW FORMAT SERDE") != -1) {
					serde_sign = 1;
				}
				if (serde_sign == 0) {
					createTmpTabSQL += tmp + "\n";
				}
				if (column_sign == 1) {
					if (tmp1.indexOf("'transactional'='true'") != -1) {
						transactional = "true";
					}
					if (tmp1.indexOf("PARTITIONED BY") != -1) {
						isPartition = "true";
					}
					if (partitioned_sign == 1) {
						String tmp2 = tmp1.trim();
						if (tmp2.indexOf(")") != -1) {
							partitioned_sign = 0;
						}
						tmp2 = tmp2.replace(")", "").replace("(", "");
						tmp2 = tmp2.trim();
						partition_columns += tmp2.trim();
						String[] tmp3 = tmp2.split(",");
						for (int n = 0; n < tmp3.length; n++) {
							String tmp5 = tmp3[n];
							partition_insert_head += tmp5.split(" ")[0] + ",";
						}
					}
					if (tmp1.indexOf("PARTITIONED BY") != -1) {
						partitioned_sign = 1;
					} // 放在后面才不会导致partition_columns不正确

					if (tmp1.indexOf("PARTITIONED BY RANGE") != -1) {
						isRangePartition = "true";
					}
					if (tmp1.indexOf("PARTITION ") != -1) {
						String PARTITION = tmp1.substring(tmp1.indexOf("PARTITION") + 9, tmp1.indexOf(" VALUES"));
						String PARTITION_VALUE = tmp1.substring(tmp1.indexOf("("), tmp1.indexOf(")") + 1);
						PARTITION = PARTITION.trim();
						PARTITION_VALUE = PARTITION_VALUE.trim();
						String[] PARTITION_NAME_VALUE = { PARTITION, PARTITION_VALUE };
						if ("all".equals(paritionsRange) || paritionsRange.indexOf(PARTITION) != -1) {
							partition_addPartition.put(PARTITION,
									"ALTER TABLE " + tab_full_name + " ADD IF NOT EXISTS PARTITION " + PARTITION
											+ " VALUES LESS THAN " + PARTITION_VALUE + ";");
							partition_list.add(PARTITION_NAME_VALUE);
							partition_dropPartition.put(PARTITION,
									"ALTER TABLE " + tab_full_name + " DROP PARTITION " + PARTITION + ";");
							partition_trunPartition.put(PARTITION,
									"truncate table " + tab_full_name + " partition " + PARTITION + ";");
						}
					}
				}
			}

			// -----------元数据解析结束end
			if ("true".equals(isRangePartition)) {// 通过partition_list拼接where字符串
				String[] partition_column = partition_columns.split(",");
				String[] partition_value_bef = null;
				HashMap<String, String> maxValueMap = new HashMap<String, String>();
				for (int i = 0; i < partition_list.size(); i++) {
					String partition_name = partition_list.get(i)[0];
					String partition_values = partition_list.get(i)[1];
					partition_values = partition_values.replace(" ", "");
					partition_values = partition_values.replace("(", "");
					partition_values = partition_values.replace(")", "");
					String[] partition_value = partition_values.split(",");
					String whereStr = "(";
					for (int j = 0; j < partition_column.length; j++) {
						String partition_column_name = partition_column[j];
						String partition_column_value = partition_value[j];
						String partition_column_bef_value = partition_value_bef != null ? partition_value_bef[j] : "";
						String partition_value_tmp = "";
						if (partition_value_bef == null || "MINVALUE".equals(partition_column_value)) {
							partition_value_tmp = partition_column_name + "<" + partition_column_value + " and ";
						} else if ("MAXVALUE".equals(partition_column_value)) {
							partition_value_tmp = partition_column_name + ">=" + partition_column_bef_value + " and ";
						} else {
							partition_value_tmp = partition_column_name + ">=" + partition_column_bef_value + " and "
									+ partition_column_name + "<" + partition_column_value + " and ";
						}
						String partition_value_bef_tmp = maxValueMap.get(partition_column_name);
						if (partition_value_bef_tmp != null
								&& partition_column_value.equals(partition_column_bef_value)) {
							whereStr += partition_value_bef_tmp;
						} else {
							whereStr += partition_value_tmp;
							maxValueMap.put(partition_column_name, partition_value_tmp);
						}
					}
					whereStr += ")";
					whereStr = whereStr.replace(" and )", ")");
					whereStr = whereStr.replace("'", "\"");
					partition_value_bef = partition_value;
					partition_whereStr.put(partition_name, whereStr);
				}
			} else {
				for (int i = 0; i < partition_list.size(); i++) {
					String[] partitions = partition_list.get(i);
					String partition_name = partitions[0];
					String whereStr = "(" + partition_name + "\")";
					whereStr = whereStr.replace("=", "=\"");
					whereStr = whereStr.replace("/", "\" and ");
					partition_whereStr.put(partition_name, whereStr);
				}
			}

			columnStr = columnStr.toLowerCase().replace("create,", "").replace("),", "");
			columnStr = columnStr.substring(0, columnStr.length() - 1);
			createTableSQL += ";";
			dropTmpTabSQL = "drop table " + tmp_tab_full_name + ";";
			if (serde_type.indexOf("LazySimpleSerDe") != -1) {
				createTmpTabSQL = createTableSQL.replace(tab_full_name, tmp_tab_full_name);
			} else {
				createTmpTabSQL = createTmpTabSQL.replace(tab_full_name, tmp_tab_full_name) + " STORED AS ORC;";
			}
			if (partition_insert_head != null && partition_insert_head.length() >= 1) {
				partition_insert_head = " PARTITION ("
						+ partition_insert_head.substring(0, partition_insert_head.length() - 1) + ") ";
				partition_insert_head = partition_insert_head.replace("'", "\"");
			}

			if ((serde_type.indexOf("OrcSerde") != -1 || serde_type.indexOf("LazySimpleSerDe") != -1)
					&& "false".equals(transactional)) {
				inputFormat = "orc";
			}
			metadataBean.setPartition_insert_head(partition_insert_head);
			metadataBean.setPartition_addPartition(partition_addPartition);
			metadataBean.setColumnStr(columnStr);
			metadataBean.setCreateTableSQL(createTableSQL);
			metadataBean.setCreateTmpTabSQL(createTmpTabSQL);
			metadataBean.setDropTmpTabSQL(dropTmpTabSQL);
			metadataBean.setPartition_dropPartition(partition_dropPartition);
			metadataBean.setParitionsRange(paritionsRange);
			metadataBean.setTab_location(tab_location);
			metadataBean.setInputFormat(inputFormat);
			metadataBean.setTransactional(transactional);
			metadataBean.setIsPartition(isPartition);
			metadataBean.setIsRangePartition(isRangePartition);
			metadataBean.setPartition_columns(partition_columns);
			metadataBean.setPartition_list(partition_list);
			metadataBean.setPartition_whereStr(partition_whereStr);
			metadataBean.setTmp_tab_full_name(tmp_tab_full_name.toLowerCase());
			metadataBean.setTemp_tab_name(temp_tab_name.toLowerCase());
			metadataBean.setPartition_trunPartition(partition_trunPartition);
			metadataBean.setTemp_db_name(tranfersDataBean.getTemp_database().toLowerCase());
			metadataBean.setTab_full_name(tab_full_name.toLowerCase());
			metadataBean.setSerde_type(serde_type);
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
			if (conn_tdh != null && !conn_tdh.isClosed()) {
				conn_tdh.close();
			}
		}
		return metadataBean;
	}
	
	
}
