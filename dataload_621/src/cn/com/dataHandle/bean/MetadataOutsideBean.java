package cn.com.dataHandle.bean;

import java.util.ArrayList;
import java.util.HashMap;

public class MetadataOutsideBean {
	private ArrayList<String[]> src_list_bef = new ArrayList<String[]>();// 上一次文件表中的字段信息列表
	private ArrayList<String[]> src_list_curr = new ArrayList<String[]>();// 文件表中的字段信息列表
	private ArrayList<String> insert_table_info_list = new ArrayList<String>();
	private HashMap<String,String> comment_info = new HashMap<String,String>();
	private int bef_size = 0;
	private int cur_size = 0;
	private String primaryKey = "";

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public int getBef_size() {
		return bef_size;
	}

	public void setBef_size(int bef_size) {
		this.bef_size = bef_size;
	}

	public int getCur_size() {
		return cur_size;
	}

	public void setCur_size(int cur_size) {
		this.cur_size = cur_size;
	}

	public ArrayList<String[]> getSrc_list_bef() {
		return src_list_bef;
	}

	public void setSrc_list_bef(ArrayList<String[]> src_list_bef) {
		this.src_list_bef = src_list_bef;
	}

	public ArrayList<String[]> getSrc_list_curr() {
		return src_list_curr;
	}

	public void setSrc_list_curr(ArrayList<String[]> src_list_curr) {
		this.src_list_curr = src_list_curr;
	}

	public ArrayList<String> getInsert_table_info_list() {
		return insert_table_info_list;
	}

	public void setInsert_table_info_list(ArrayList<String> insert_table_info_list) {
		this.insert_table_info_list = insert_table_info_list;
	}

	public HashMap<String, String> getComment_info() {
		return comment_info;
	}

	public void setComment_info(HashMap<String, String> comment_info) {
		this.comment_info = comment_info;
	}
	
    
}
