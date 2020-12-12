package cn.com.dataHandle.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MetadataTDHBean {
	private int result = 0;
	private String createTableSQL = "";
	private String createTmpTabSQL = "";
	private String dropTmpTabSQL = "";
	private String tab_location = "";
	private String paritionsSQL = "";
	private String paritionsRange = "";
	private String columnStr = "";
	private String partition_insert_head = "";
	private String inputFormat = "";
	private String transactional = "";
	private String isPartition = "";
	private String partition_columns = "";
	private String isRangePartition = "";
	private String temp_db_name = "";
	private String temp_tab_name = "";
	private String tmp_tab_full_name = "";
	private String tab_full_name = "";
	private String serde_type = "";
	private String tmp_tab_location = "";
	private ArrayList<String[]> tar_columns_list = new ArrayList<String[]>();//存放目标表中的字段信息
	private ArrayList<String[]> src_columns_list = new ArrayList<String[]>();//存放源表中的字段信息
	private List<String[]> partition_list = new ArrayList<String[]>();
	HashMap<String, String> partition_whereStr = new HashMap<String, String>();
	HashMap<String, String> partition_dropPartition = new HashMap<String, String>();
	HashMap<String, String> partition_addPartition = new HashMap<String, String>();
	HashMap<String, String> partition_trunPartition = new HashMap<String, String>();

	public int getResult() {
		return result;
	}

	public void setResult(int result) {
		this.result = result;
	}

	public String getTmp_tab_location() {
		return tmp_tab_location;
	}

	public void setTmp_tab_location(String tmp_tab_location) {
		this.tmp_tab_location = tmp_tab_location;
	}

	public String getSerde_type() {
		return serde_type;
	}

	public void setSerde_type(String serde_type) {
		this.serde_type = serde_type;
	}

	public String getTab_full_name() {
		return tab_full_name;
	}

	public void setTab_full_name(String tab_full_name) {
		this.tab_full_name = tab_full_name;
	}

	public String getTemp_db_name() {
		return temp_db_name;
	}

	public void setTemp_db_name(String temp_db_name) {
		this.temp_db_name = temp_db_name;
	}

	public HashMap<String, String> getPartition_trunPartition() {
		return partition_trunPartition;
	}

	public void setPartition_trunPartition(HashMap<String, String> partition_trunPartition) {
		this.partition_trunPartition = partition_trunPartition;
	}

	public HashMap<String, String> getPartition_whereStr() {
		return partition_whereStr;
	}

	public void setPartition_whereStr(HashMap<String, String> partition_whereStr) {
		this.partition_whereStr = partition_whereStr;
	}

	public String getTemp_tab_name() {
		return temp_tab_name;
	}

	public void setTemp_tab_name(String temp_tab_name) {
		this.temp_tab_name = temp_tab_name;
	}


	public String getIsPartition() {
		return isPartition;
	}

	public void setIsPartition(String isPartition) {
		this.isPartition = isPartition;
	}

	public String getPartition_columns() {
		return partition_columns;
	}

	public void setPartition_columns(String partition_columns) {
		this.partition_columns = partition_columns;
	}

	public String getIsRangePartition() {
		return isRangePartition;
	}

	public void setIsRangePartition(String isRangePartition) {
		this.isRangePartition = isRangePartition;
	}

	public String getPartition_insert_head() {
		return partition_insert_head;
	}

	public void setPartition_insert_head(String partition_insert_head) {
		this.partition_insert_head = partition_insert_head;
	}

	public String getInputFormat() {
		return inputFormat;
	}

	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	public String getTransactional() {
		return transactional;
	}

	public void setTransactional(String transactional) {
		this.transactional = transactional;
	}

	public String getCreateTableSQL() {
		return createTableSQL;
	}

	public void setCreateTableSQL(String createTableSQL) {
		this.createTableSQL = createTableSQL;
	}

	public String getCreateTmpTabSQL() {
		return createTmpTabSQL;
	}

	public void setCreateTmpTabSQL(String createTmpTabSQL) {
		this.createTmpTabSQL = createTmpTabSQL;
	}

	public String getDropTmpTabSQL() {
		return dropTmpTabSQL;
	}

	public void setDropTmpTabSQL(String dropTmpTabSQL) {
		this.dropTmpTabSQL = dropTmpTabSQL;
	}

	public String getTab_location() {
		return tab_location;
	}

	public void setTab_location(String tab_location) {
		this.tab_location = tab_location;
	}

	public String getParitionsSQL() {
		return paritionsSQL;
	}

	public void setParitionsSQL(String paritionsSQL) {
		this.paritionsSQL = paritionsSQL;
	}

	public String getParitionsRange() {
		return paritionsRange;
	}

	public void setParitionsRange(String paritionsRange) {
		this.paritionsRange = paritionsRange;
	}

	public String getColumnStr() {
		return columnStr;
	}

	public void setColumnStr(String columnStr) {
		this.columnStr = columnStr;
	}

	public HashMap<String, String> getPartition_dropPartition() {
		return partition_dropPartition;
	}

	public void setPartition_dropPartition(HashMap<String, String> partition_dropPartition) {
		this.partition_dropPartition = partition_dropPartition;
	}

	public HashMap<String, String> getPartition_addPartition() {
		return partition_addPartition;
	}

	public void setPartition_addPartition(HashMap<String, String> partition_addPartition) {
		this.partition_addPartition = partition_addPartition;
	}

	public List<String[]> getPartition_list() {
		return partition_list;
	}

	public void setPartition_list(List<String[]> partition_list) {
		this.partition_list = partition_list;
	}

	public String getTmp_tab_full_name() {
		return tmp_tab_full_name;
	}

	public void setTmp_tab_full_name(String tmp_tab_full_name) {
		this.tmp_tab_full_name = tmp_tab_full_name;
	}

	public ArrayList<String[]> getTar_columns_list() {
		return tar_columns_list;
	}

	public void setTar_columns_list(ArrayList<String[]> tar_columns_list) {
		this.tar_columns_list = tar_columns_list;
	}

	public ArrayList<String[]> getSrc_columns_list() {
		return src_columns_list;
	}

	public void setSrc_columns_list(ArrayList<String[]> src_columns_list) {
		this.src_columns_list = src_columns_list;
	}

	
}
