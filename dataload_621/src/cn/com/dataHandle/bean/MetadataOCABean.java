package cn.com.dataHandle.bean;

import java.util.ArrayList;

public class MetadataOCABean {
	private int result = 0;
	private String createTableSQL = "";
    private String truncateTabSQL = "";
	private String dropTabSQL = "";
	private String tab_full_name = "";
	private String crtlSQL="";
	private ArrayList<String[]> tar_columns_list = new ArrayList<String[]>();//存放目标表中的字段信息
	private ArrayList<String[]> src_columns_list = new ArrayList<String[]>();//存放源表中的字段信息
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
	public String getCrtlSQL() {
		return crtlSQL;
	}
	public void setCrtlSQL(String crtlSQL) {
		this.crtlSQL = crtlSQL;
	}
	public int getResult() {
		return result;
	}
	public void setResult(int result) {
		this.result = result;
	}
	public String getCreateTableSQL() {
		return createTableSQL;
	}
	public void setCreateTableSQL(String createTableSQL) {
		this.createTableSQL = createTableSQL;
	}
	public String getTruncateTabSQL() {
		return truncateTabSQL;
	}
	public void setTruncateTabSQL(String truncateTabSQL) {
		this.truncateTabSQL = truncateTabSQL;
	}
	public String getDropTabSQL() {
		return dropTabSQL;
	}
	public void setDropTabSQL(String dropTabSQL) {
		this.dropTabSQL = dropTabSQL;
	}
	public String getTab_full_name() {
		return tab_full_name;
	}
	public void setTab_full_name(String tab_full_name) {
		this.tab_full_name = tab_full_name;
	}
	
	
}
