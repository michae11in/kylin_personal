package cn.com.dataHandle.bean;

import java.util.HashMap;

public class DataLoadBean {

	private String job_id = "";
	private String tactics_id = "";
	private String src_sys_id = "";
	private String src_table_type = "";
	private String src_file_name = "";
	private String db_name_txt = "";
	private String table_name = "";
	private String db_name_orc = "";
	private String table_name_orc = "";
	private String table_name_txt = "";
	private String load_condition = "";
	private String load_type = "";
	private String col_map_type = "";

	private String authUserTDH = "";
	private String authUserMysql = "";
	private String conf_hdfs = "";
	private String login_kerberos = "";
	private String delimited = "";
	private String dataFile_hist_dir = "";
	private String etl_date = "";
	private String thread_id = "";
	private String batch_id = "";
	private String usr_def_partition="";
	private String def_etl_date="";
	
	private HashMap<String, HashMap<String,String>> columnMap = new HashMap<String, HashMap<String,String>>();

	private String table_full_name_orc = "";
	private String table_full_name_txt = "";
	private String dataFile_curr_dir = "";
	private String log_file_dir ="";
	private String datafile_curr_txt_file = "";
	private String datafile_curr_end_file = "";
	private String datafile_curr_sql_file = "";
	private String datafile_hist_txt_file = "";
	private String datafile_hist_end_file = "";
	private String datafile_hist_sql_file = "";
	
	private int type = -1;
	private int job_tar_count = -1;
	private int job_src_count = -1;
	private int job_used_time = -1;
	private long startTime = 0;
	private String log_id ="";
	
	//初始化参数
	private long txt_file_size = 0;	
	private long eve_row_size = 0;
	private long partition_size = 0;
	private String partition_sql = "";
	private int init_bucket_size=90;

	public String getLog_file_dir() {
		return log_file_dir;
	}

	public void setLog_file_dir(String log_file_dir) {
		this.log_file_dir = log_file_dir;
	}

	public String getTable_full_name_txt() {
		return table_full_name_txt;
	}

	public void setTable_full_name_txt(String table_full_name_txt) {
		this.table_full_name_txt = table_full_name_txt;
	}

	public String getLoad_condition() {
		return load_condition;
	}

	public void setLoad_condition(String load_condition) {
		this.load_condition = load_condition;
	}

	public String getTable_name_txt() {
		return table_name_txt;
	}

	public void setTable_name_txt(String table_name_txt) {
		this.table_name_txt = table_name_txt;
	}

	public String getTable_full_name_orc() {
		return table_full_name_orc;
	}

	public void setTable_full_name_orc(String table_full_name_orc) {
		this.table_full_name_orc = table_full_name_orc;
	}

	public String getSrc_file_name() {
		return src_file_name;
	}

	public void setSrc_file_name(String src_file_name) {
		this.src_file_name = src_file_name;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public int getJob_tar_count() {
		return job_tar_count;
	}

	public void setJob_tar_count(int job_tar_count) {
		this.job_tar_count = job_tar_count;
	}

	public int getJob_src_count() {
		return job_src_count;
	}

	public void setJob_src_count(int job_src_count) {
		this.job_src_count = job_src_count;
	}

	public HashMap<String, HashMap<String,String>> getColumnMap() {
		return columnMap;
	}

	public void setColumnMap(HashMap<String, HashMap<String,String>> columnMap) {
		this.columnMap = columnMap;
	}
	
	public String getCol_map_type() {
		return col_map_type;
	}

	public void setCol_map_type(String col_map_type) {
		this.col_map_type = col_map_type;
	}


	public int getJob_used_time() {
		return job_used_time;
	}

	public void setJob_used_time(int job_used_time) {
		this.job_used_time = job_used_time;
	}

	public String getDelimited() {
		return delimited;
	}

	public void setDelimited(String delimited) {
		this.delimited = delimited;
	}

	public String getDatafile_hist_txt_file() {
		return datafile_hist_txt_file;
	}

	public void setDatafile_hist_txt_file(String datafile_hist_txt_file) {
		this.datafile_hist_txt_file = datafile_hist_txt_file;
	}

	public String getDatafile_hist_end_file() {
		return datafile_hist_end_file;
	}

	public void setDatafile_hist_end_file(String datafile_hist_end_file) {
		this.datafile_hist_end_file = datafile_hist_end_file;
	}

	public String getDatafile_hist_sql_file() {
		return datafile_hist_sql_file;
	}

	public void setDatafile_hist_sql_file(String datafile_hist_sql_file) {
		this.datafile_hist_sql_file = datafile_hist_sql_file;
	}

	public String getLoad_type() {
		return load_type;
	}

	public void setLoad_type(String load_type) {
		this.load_type = load_type;
	}

	public String getDataFile_hist_dir() {
		return dataFile_hist_dir;
	}

	public void setDataFile_hist_dir(String dataFile_hist_dir) {
		this.dataFile_hist_dir = dataFile_hist_dir;
	}

	public String getThread_id() {
		return thread_id;
	}

	public void setThread_id(String thread_id) {
		this.thread_id = thread_id;
	}

	public String getDatafile_curr_txt_file() {
		return datafile_curr_txt_file;
	}

	public void setDatafile_curr_txt_file(String datafile_curr_txt_file) {
		this.datafile_curr_txt_file = datafile_curr_txt_file;
	}

	public String getDatafile_curr_sql_file() {
		return datafile_curr_sql_file;
	}

	public void setDatafile_curr_sql_file(String datafile_curr_sql_file) {
		this.datafile_curr_sql_file = datafile_curr_sql_file;
	}

	public String getJob_id() {
		return job_id;
	}

	public void setJob_id(String job_id) {
		this.job_id = job_id;
	}

	public String getTactics_id() {
		return tactics_id;
	}

	public void setTactics_id(String tactics_id) {
		this.tactics_id = tactics_id;
	}

	public String getDatafile_curr_end_file() {
		return datafile_curr_end_file;
	}

	public void setDatafile_curr_end_file(String datafile_curr_end_file) {
		this.datafile_curr_end_file = datafile_curr_end_file;
	}

	public String getConf_hdfs() {
		return conf_hdfs;
	}

	public void setConf_hdfs(String conf_hdfs) {
		this.conf_hdfs = conf_hdfs;
	}

	public String getLogin_kerberos() {
		return login_kerberos;
	}

	public void setLogin_kerberos(String login_kerberos) {
		this.login_kerberos = login_kerberos;
	}

	public String getAuthUserTDH() {
		return authUserTDH;
	}

	public void setAuthUserTDH(String authUserTDH) {
		this.authUserTDH = authUserTDH;
	}
   
	public String getUsr_def_partition() {
		return usr_def_partition;
	}

	public void setUsr_def_partition(String usr_def_partition) {
		this.usr_def_partition = usr_def_partition;
	}
	

	public String getDef_etl_date() {
		return def_etl_date;
	}

	public void setDef_etl_date(String def_etl_date) {
		this.def_etl_date = def_etl_date;
	}

	public String getAuthUserMysql() {
		return authUserMysql;
	}

	public void setAuthUserMysql(String authUserMysql) {
		this.authUserMysql = authUserMysql;
	}

	public String getBatch_id() {
		return batch_id;
	}

	public void setBatch_id(String batch_id) {
		this.batch_id = batch_id;
	}

	public String getSrc_sys_id() {
		return src_sys_id;
	}

	public void setSrc_sys_id(String src_sys_id) {
		this.src_sys_id = src_sys_id;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public String getTable_name_orc() {
		return table_name_orc;
	}

	public void setTable_name_orc(String table_name_orc) {
		this.table_name_orc = table_name_orc;
	}

	public String getDb_name_txt() {
		return db_name_txt;
	}

	public void setDb_name_txt(String db_name_txt) {
		this.db_name_txt = db_name_txt;
	}

	public String getDb_name_orc() {
		return db_name_orc;
	}

	public void setDb_name_orc(String db_name_orc) {
		this.db_name_orc = db_name_orc;
	}

	public String getEtl_date() {
		return etl_date;
	}

	public void setEtl_date(String etl_date) {
		this.etl_date = etl_date;
	}

	public String getDataFile_curr_dir() {
		return dataFile_curr_dir;
	}

	public void setDataFile_curr_dir(String dataFile_curr_dir) {
		this.dataFile_curr_dir = dataFile_curr_dir;
	}

	public String getSrc_table_type() {
		return src_table_type;
	}

	public void setSrc_table_type(String src_table_type) {
		this.src_table_type = src_table_type;
	}

	public long getTxt_file_size() {
		return txt_file_size;
	}

	public void setTxt_file_size(long txt_file_size) {
		this.txt_file_size = txt_file_size;
	}

	public long getEve_row_size() {
		return eve_row_size;
	}

	public void setEve_row_size(long eve_row_size) {
		this.eve_row_size = eve_row_size;
	}

	public String getPartition_sql() {
		return partition_sql;
	}

	public void setPartition_sql(String partition_sql) {
		this.partition_sql = partition_sql;
	}

	public int getInit_bucket_size() {
		return init_bucket_size;
	}

	public void setInit_bucket_size(int init_bucket_size) {
		this.init_bucket_size = init_bucket_size;
	}

	public long getPartition_size() {
		return partition_size;
	}

	public void setPartition_size(long partition_size) {
		this.partition_size = partition_size;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getLog_id() {
		return log_id;
	}

	public void setLog_id(String log_id) {
		this.log_id = log_id;
	}


	

}
