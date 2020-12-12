package cn.com.dataHandle.bean;

public class TranfersDataBean {
	private int result = 0;
	private int type = -1;
	private String mysql_login_ldap = "";
	private String tdh_login_ldap = "";
	private String home_path = "";
	private String conf_hdfs = "";
	private String login_kerberos = "";
	private int parall_spot = 5;
	private int re_run = 3;
	private String exec_type = "";
	private String exec_sql = "";
	private String etl_date = "";
	private String log_file_dir = "";

	private String job_id = "";
	private String tactics_id = "";
	private String db_name = "";
	private String tab_name = "";
	private String lock_table = "";
	private String remove_fields = "";
	private String tab_full_name = "";
	private String temp_database = "";
	private String tmp_tab_full_name = "";
	private String tabInfo_file_path = "";
	// 以下值是在程序中间生成
	private String insertField = "";
	private String alltxtField = "";
	private String data_file_path = "";
	private String min_file_path = "";
	private int out_times = -1;

	private String batch_id = "";
	private long startTime = 0;
	private int job_src_count = -1;
	private int job_tar_count = -1;
	private  String log_id = "";
	
	public String getTabInfo_file_path() {
		return tabInfo_file_path;
	}

	public void setTabInfo_file_path(String tabInfo_file_path) {
		this.tabInfo_file_path = tabInfo_file_path;
	}

	public int getResult() {
		return result;
	}

	public void setResult(int result) {
		this.result = result;
	}

	public String getTmp_tab_full_name() {
		return tmp_tab_full_name;
	}

	public void setTmp_tab_full_name(String tmp_tab_full_name) {
		this.tmp_tab_full_name = tmp_tab_full_name;
	}

	private String src_tab_full_name = "";

	public String getSrc_tab_full_name() {
		return src_tab_full_name;
	}

	public void setSrc_tab_full_name(String src_tab_full_name) {
		this.src_tab_full_name = src_tab_full_name;
	}

	public String getLog_file_dir() {
		return log_file_dir;
	}

	public void setLog_file_dir(String log_file_dir) {
		this.log_file_dir = log_file_dir;
	}

	public int getJob_src_count() {
		return job_src_count;
	}

	public void setJob_src_count(int job_src_count) {
		this.job_src_count = job_src_count;
	}

	public int getJob_tar_count() {
		return job_tar_count;
	}

	public void setJob_tar_count(int job_tar_count) {
		this.job_tar_count = job_tar_count;
	}

	public String getBatch_id() {
		return batch_id;
	}

	public void setBatch_id(String batch_id) {
		this.batch_id = batch_id;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public String getExec_type() {
		return exec_type;
	}

	public void setExec_type(String exec_type) {
		this.exec_type = exec_type;
	}

	public String getExec_sql() {
		return exec_sql;
	}

	public void setExec_sql(String exec_sql) {
		this.exec_sql = exec_sql;
	}

	public int getOut_times() {
		return out_times;
	}

	public void setOut_times(int out_times) {
		this.out_times = out_times;
	}

	public String getMin_file_path() {
		return min_file_path;
	}

	public void setMin_file_path(String min_file_path) {
		this.min_file_path = min_file_path;
	}

	public String getData_file_path() {
		return data_file_path;
	}

	public void setData_file_path(String data_file_path) {
		this.data_file_path = data_file_path;
	}

	public String getTemp_database() {
		return temp_database;
	}

	public void setTemp_database(String temp_database) {
		this.temp_database = temp_database;
	}

	public String getLock_table() {
		return lock_table;
	}

	public void setLock_table(String lock_table) {
		this.lock_table = lock_table;
	}

	public String getEtl_date() {
		return etl_date;
	}

	public void setEtl_date(String etl_date) {
		this.etl_date = etl_date;
	}

	public String getTab_full_name() {
		return tab_full_name;
	}

	public void setTab_full_name(String tab_full_name) {
		this.tab_full_name = tab_full_name;
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

	public String getDb_name() {
		return db_name;
	}

	public void setDb_name(String db_name) {
		this.db_name = db_name;
	}

	public String getTab_name() {
		return tab_name;
	}

	public void setTab_name(String tab_name) {
		this.tab_name = tab_name;
	}

	public String getMysql_login_ldap() {
		return mysql_login_ldap;
	}

	public void setMysql_login_ldap(String mysql_login_ldap) {
		this.mysql_login_ldap = mysql_login_ldap;
	}

	public String getTdh_login_ldap() {
		return tdh_login_ldap;
	}

	public void setTdh_login_ldap(String tdh_login_ldap) {
		this.tdh_login_ldap = tdh_login_ldap;
	}

	public String getHome_path() {
		return home_path;
	}

	public void setHome_path(String home_path) {
		this.home_path = home_path;
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

	public int getParall_spot() {
		return parall_spot;
	}

	public void setParall_spot(int parall_spot) {
		this.parall_spot = parall_spot;
	}

	public int getRe_run() {
		return re_run;
	}

	public void setRe_run(int re_run) {
		this.re_run = re_run;
	}

	public String getRemove_fields() {
		return remove_fields;
	}

	public void setRemove_fields(String remove_fields) {
		this.remove_fields = remove_fields;
	}

	public String getInsertField() {
		return insertField;
	}

	public void setInsertField(String insertField) {
		this.insertField = insertField;
	}

	public String getAlltxtField() {
		return alltxtField;
	}

	public void setAlltxtField(String alltxtField) {
		this.alltxtField = alltxtField;
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
