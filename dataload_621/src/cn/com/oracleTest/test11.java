package cn.com.oracleTest;

import java.io.File;

public class test11 {
	public static void main(String[] args) {
		  File file=new File("D://export//use_wft//app//tdh_tools//expData//20201117\\test.ciyutest\\data\\\\000000_0"); //指定文件名及路径
		  String name="D://export//use_wft//app//tdh_tools//expData//20201117\\\\test.ciyutest\\\\data\\\\\\\\000000_0";
		//  String filename=file.getAbsolutePath();
//		  if(filename.indexOf(".")>=0)
//		  {
//		   filename = filename.substring(0,filename.lastIndexOf("."));
//		  }
		 boolean a= file.renameTo(new File(name+".txt")); //改名
		 System.out.println(a);
		 }
}
