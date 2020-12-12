package cn.com.oracleTest;

import java.util.ArrayList;
import java.util.List;
	import java.io.BufferedReader;
	import java.io.BufferedWriter;
	import java.io.File;
	import java.io.FileInputStream;
	import java.io.FileOutputStream;
	import java.io.InputStream;
	import java.io.InputStreamReader;
	import java.io.OutputStream;
	import java.io.OutputStreamWriter;
	import java.text.DecimalFormat;
	import java.util.Arrays;
	import java.util.Date;
	import java.util.UUID;

	import org.apache.hadoop.fs.FileStatus;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IOUtils;

	import cn.com.pub.PubException;
import cn.com.transferFile.TransferFilePub;
import cn.com.pub.PubAPIs;

	public class test {
		private PubAPIs pubAPIs = new PubAPIs("test");
		private String thread_id = UUID.randomUUID().toString().replace("-", "").replace(".", "");
		private int m = 0;
		private String SMkey;
		private int bfSize = 4096;

		private String lEcode;
		private int lpushThreadNum;
		private String lfileType;
		private String lsysType;
		private FileSystem fs_l;

		private short rReplication = 3;
		private String rForceDel;
		private String rEcode;
		private int rcutSize;
		private int fileSize;
		private String rsysType;
		private String rCRYPT;
		private String classname = "";
		private String l_loginT = "";
		private String lconf_defT = "";
		private String r_loginT = "";
		private String rconf_defT = "";

		public void setPubAPIs_className(String className) {
			this.pubAPIs.setClassName(className);
		}

		private int out_times = -1;

		public void setOut_times(int out_times) {
			this.out_times = out_times;
		}

		public test(String classname) {
			this.classname = classname;
			pubAPIs.LoadProfile(classname);
		}

		public int transferCatalog(String l_loginT, String lconf_defT, String lfilePathT, String r_loginT,
				String rconf_defT, String rfilePathT, String table_name) {
			pubAPIs.writerLog("TransferFiles transferCatalog:" + lfilePathT + "->" + rfilePathT);
			m = 0;
			int result = 0;
			try {
				this.l_loginT = l_loginT;
				this.lconf_defT = lconf_defT;
				this.r_loginT = r_loginT;
				this.rconf_defT = rconf_defT;
				init();
				if (rcutSize < 10 && rcutSize != 0) {
					throw new PubException("文件的最小切割大小为10*1000*1000个字符，注意汉字占2个字节算1个符");
				}
				fileSize = rcutSize * 1000 * 1000;
				String[] lfilePaths = lfilePathT.split(",");
				String[] rfilePaths = rfilePathT.split(",");
				if (lfilePaths.length != rfilePaths.length || lfilePaths.length == 0 || rfilePaths.length == 0) {
					throw new PubException("Parameter incorrect!");
				}
				for (int i = 0; i < lfilePaths.length; i++) {
					putBatchFile(lfilePaths[i], rfilePaths[i]);
				}

				Thread.sleep(500);
				int n = 0;
				while (TransferFilePub.threadLock(thread_id, "get") != 0) {
					n++;
					if (n > 10800) {// 异常情况下主程序在等3个小时之后会自动退出
						break;
					}
					Thread.sleep(1000);
				}
				// PubAPIs.threadLock(thread_id, "del");
				if (fs_l != null) {
					// fs_l.close();
				}
				int result1 = TransferFilePub.threadCheckPushThread(thread_id, "get")[1];
				if (result1 != m) {
					String errorInfo = "数据传输过程某个文件失败!";
					throw new PubException(errorInfo);
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerLog(eStr);
				result = -1;
			}
			return result;
		}

		public int transferCatalogFromTDH(String l_loginT, String lconf_defT, String lfilePathT, String rfilePathT) {
			pubAPIs.writerLog("TransferFiles transferCatalogFromTDH:" + lfilePathT + "->" + rfilePathT);
			m = 0;
			int result = 0;
			try {
				this.l_loginT = l_loginT;
				this.lconf_defT = lconf_defT;
				this.r_loginT = "type;username`username;hive";
				this.rconf_defT = "sysType;linux`Ecode;utf-8";
				init();
				if (rcutSize < 10 && rcutSize != 0) {
					throw new PubException("文件的最小切割大小为10*1000*1000个字符，注意汉字占2个字节算1个符");
				}
				fileSize = rcutSize * 1000 * 1000;
				String[] lfilePaths = lfilePathT.split(",");
				System.out.println("lfilePaths"+lfilePaths);
				String[] rfilePaths = rfilePathT.split(",");
				System.out.println("rfilePaths"+rfilePaths);
				if (lfilePaths.length != rfilePaths.length || lfilePaths.length == 0 || rfilePaths.length == 0) {
					throw new PubException("Parameter incorrect!");
				}
				for (int i = 0; i < lfilePaths.length; i++) {
					putBatchFile(lfilePaths[i], rfilePaths[i]);
				}

				Thread.sleep(500);
				int n = 0;
				while (TransferFilePub.threadLock(thread_id, "get") != 0) {
					n++;
					if (n > out_times && out_times != -1) {// 异常情况下主程序在等N个小时之后会自动退出
						pubAPIs.writerLog("TransferCatalogFromTDH程序在等待:" + out_times + "s之后退出!");
						break;
					}
					Thread.sleep(1000);
				}
				
				// PubAPIs.threadLock(thread_id, "del");
				if (fs_l != null) {
					PubAPIs.getFileSystem("", "", classname,"close",fs_l);
				}
				int result1 = TransferFilePub.threadCheckPushThread(thread_id, "get")[1];
				if (result1 != m) {
					String errorInfo = "数据传输过程某个文件失败!";
					throw new PubException(errorInfo);
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerErrorLog(eStr);
				result = -1;
			}
			return result;
		}

		public int transferCatalogToTDH(String r_loginT, String rconf_defT, String lfilePathT, String rfilePathT, String table_name) {
			pubAPIs.writerLog("TransferFiles transferCatalogToTDH:" + lfilePathT + "->" + rfilePathT);
			m = 0;
			int result = 0;
			try {
				this.l_loginT = "type;username`username;hive";
				this.lconf_defT = "sysType;linux`Ecode;utf-8";
				this.r_loginT = r_loginT;
				this.rconf_defT = rconf_defT;
				init();
				if (rcutSize < 10 && rcutSize != 0) {
					throw new PubException("文件的最小切割大小为10*1000*1000个字符，注意汉字占2个字节算1个符");
				}
				fileSize = rcutSize * 1000 * 1000;
				String[] lfilePaths = lfilePathT.split(",");
				String[] rfilePaths = rfilePathT.split(",");
				if (lfilePaths.length != rfilePaths.length || lfilePaths.length == 0 || rfilePaths.length == 0) {
					throw new PubException("Parameter incorrect!");
				}
				for (int i = 0; i < lfilePaths.length; i++) {
					putBatchFile(lfilePaths[i], rfilePaths[i]);
				}

				Thread.sleep(500);
				int n = 0;
				while (TransferFilePub.threadLock(thread_id, "get") != 0) {
					n++;
					if (n > out_times && out_times != -1) {// 异常情况下主程序在等N个小时之后会自动退出
						pubAPIs.writerLog("TransferCatalogFromTDH程序在等待:" + out_times + "s之后退出!");
						break;
					}
					Thread.sleep(1000);
				}
				// PubAPIs.threadLock(thread_id, "del");
				if (fs_l != null) {
					PubAPIs.getFileSystem("", "", classname,"close",fs_l);
				}
				int result1 = TransferFilePub.threadCheckPushThread(thread_id, "get")[1];
				if (result1 != m) {
					String errorInfo = "数据传输过程某个文件失败!";
					throw new PubException(errorInfo);
				}
			} catch (Exception e) {
				String eStr = pubAPIs.getException(e);
				pubAPIs.writerErrorLog(eStr);
				result = -1;
			}
			return result;
		}

		private void init() throws Exception {
			SMkey = pubAPIs.getProperty("SMkey", "1,2,3,4,5,6,7,8,9,0,e,&,p,q,o,v");
			lfileType = pubAPIs.getProperty("lfileType", "KEEP");
			rForceDel = pubAPIs.getProperty("rForceDel", "true");
			rCRYPT = pubAPIs.getProperty("CRYPT", "2");

			String rReplicationTmp = pubAPIs.getProperty("rReplication", "3");
			rReplication = Short.parseShort(rReplicationTmp);
			String lpushThreadNumTmp = pubAPIs.getProperty("lpushThreadNum", "1");
			lpushThreadNum = Integer.parseInt(lpushThreadNumTmp);
			String rcutSizeTmp = pubAPIs.getProperty("rcutSize", "110");
			rcutSize = Integer.parseInt(rcutSizeTmp);
			String bfSizeStr = pubAPIs.getProperty("bfSize", "4096");
			bfSize = Integer.parseInt(bfSizeStr);

			lsysType = pubAPIs.getProperty2(lconf_defT, "sysType", "`", ";");
			rsysType = pubAPIs.getProperty2(rconf_defT, "sysType", "`", ";");
			lEcode = pubAPIs.getProperty2(lconf_defT, "Ecode", "`", ";");
			if ("".endsWith(lEcode)) {
				lEcode = "utf-8";
			}
			rEcode = pubAPIs.getProperty2(rconf_defT, "Ecode", "`", ";");
			if ("".endsWith(rEcode)) {
				rEcode = "utf-8";
			}

			if ("tdh".equals(lsysType)) {
				fs_l = PubAPIs.getFileSystem(this.lconf_defT, this.l_loginT, classname,"get",null);
			}
		}

		private void putBatchFile(String lfilePathT, String rfilePathT) throws Exception {
			boolean isDir = false;
			String fileFullPath = "";
			String rFullPath = "";
			lfilePathT = lfilePathT.replace("hdfs://nameservice1", "");
			rfilePathT = rfilePathT.replace("hdfs://nameservice1", "");
			List<String> fileNamelist = new ArrayList<String>();
			if ("tdh".equals(lsysType)) {
				if (!fs_l.exists(new Path(lfilePathT))) {
					throw new PubException(lfilePathT + " no exists");
				}
				isDir = fs_l.isDirectory(new Path(lfilePathT));
				if (isDir) {
					if (!pubAPIs.hasDirSign(lfilePathT)) {
						lfilePathT = lfilePathT + "/";
					}
					FileStatus fileStatus[] = fs_l.globStatus(new Path(lfilePathT + "*"));
					int leg = fileStatus.length;
					for (int i = 0; i < leg; i++) {
						String file_path = fileStatus[i].getPath().toString().replace("hdfs://nameservice1", "");
						fileNamelist.add(file_path);
					}
				}
			} else {
				File file = new File(lfilePathT);
				isDir = file.isDirectory();
				if (isDir) {
					File files[] = file.listFiles();
					for (int j = 0; j < files.length; j++) {
						String file_path = files[j].getAbsolutePath();
						fileNamelist.add(file_path);
					}
				}
			}
			if (!isDir) {
				fileFullPath = lfilePathT;
				if (pubAPIs.hasDirSign(rfilePathT)) {
					Path fPath = new Path(fileFullPath);
					System.out.println(rfilePathT);
					System.out.println("fPath.getName():"+fPath.getName());
					rFullPath = rfilePathT + fPath.getName();
				} else {
					System.out.println("rfilePathT:"+rfilePathT);
					rFullPath = rfilePathT;
					System.out.println("rFullPath:"+rFullPath);
				}
				PushThread rushThread = new PushThread(fileFullPath, rFullPath, thread_id);
				TransferFilePub.threadLock(thread_id, "add");
				m++;
				rushThread.start();
				while (TransferFilePub.threadLock(thread_id, "get") >= lpushThreadNum) {
					Thread.sleep(500);
				}
			} else {
				if (!pubAPIs.hasDirSign(rfilePathT)) {
					if ("tdh".equals(rsysType)) {
						rfilePathT = rfilePathT + "/";
					} else {
						rfilePathT = rfilePathT + PubAPIs.getDirSign();
					}
				}
				for (int i = 0; i < fileNamelist.size(); i++) {
					fileFullPath = fileNamelist.get(i);
					Path fPath = new Path(fileFullPath);
					System.out.println("fPath.getName();"+fPath.getName());
					rFullPath = rfilePathT + fPath.getName();
					putBatchFile(fileFullPath, rFullPath);
				}
			}
		}

		private class PushThread extends Thread {
			private String TlfilePath;
			private String TrFlie;

			private FileSystem Tfs_l;
			private FileSystem Tfs_r;

			private long l_fileSize = -1;
			private long r_fileSize = -1;
			String thread_id = "";

			public PushThread(String plfilePathT, String prFlieT, String thread_id) {
				this.TlfilePath = plfilePathT;
				this.TrFlie = prFlieT;
				this.thread_id = thread_id;
			}

			public void run() {
				int result = 0;
				long start = new Date().getTime();
				int threads = TransferFilePub.threadLock(thread_id, "get");
				pubAPIs.writerLog(" Start:" + TlfilePath + " --> " + TrFlie + ". Running Threads:" + threads);
				for (int i = 0; i < 3; i++) {
					result = run_ft();
					if (result == 0) {
						TransferFilePub.threadCheckPushThread(thread_id, "add2");
						break;
					} else {
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							String eStr = pubAPIs.getException(e);
							pubAPIs.writerLog("Thread.sleep:" + eStr);
						}
					}
				}
				long end = new Date().getTime();
				long useTime = (end - start) / 1000;
				double fileSize = l_fileSize * 1.0 / 1024 / 1024;
				DecimalFormat df = new DecimalFormat("#0.00");
				pubAPIs.writerLog("Finish:" + TlfilePath + " --> " + TrFlie + ". FileSize:" + df.format(fileSize) + "M。Use:"
						+ useTime + "s");
				TransferFilePub.threadLock(thread_id, "minus");
			}

			public int run_ft() {
				int result = 0;
				try {
					if ("".equals(TlfilePath) || "".equals(TrFlie)) {
						throw new PubException("请设置TlfilePath、TrFlie的值!");
					}

					// 定义源数据流
					if ("tdh".equals(lsysType)) {
						Tfs_l = PubAPIs.getFileSystem(lconf_defT, l_loginT, classname,"get",null);
					}
					BufferedReader fis = null;
					InputStream in = null;
					if (!"KEEP".equals(lfileType)) {
						fis = openInputStream();
					} else {
						in = openInputStreamKeep();
					}
					if ("tdh".equals(lsysType)) {
						l_fileSize = Tfs_l.getContentSummary(new Path(TlfilePath)).getLength();
					} else {
						l_fileSize = new File(TlfilePath).length();
					}

					// 定义目标流
					if ("tdh".equals(rsysType)) {
						Tfs_r = PubAPIs.getFileSystem(rconf_defT, r_loginT, classname,"get",null);
					}
					BufferedWriter fos = null;
					OutputStream out = null;
					String TrFlie_fullName = "";
					String suffix = "";
					int m = 100000;

					if ("tdh".equals(rsysType)) {
						if ("true".equals(rForceDel)) {
							Tfs_r.delete(new Path(TrFlie), true);
						} else {
							if (Tfs_r.exists(new Path(TrFlie))) {
								throw new PubException(TrFlie + " exists!");
							}
						}
						if (rcutSize > 0 && ("CSV".equals(lfileType) || "TXT".equals(lfileType))
								&& "tdh".equals(rsysType)) {
							TrFlie_fullName = TrFlie + "/" + m + suffix;
							System.out.println("TrFlie_fullName1"+TrFlie_fullName);
						} else {
							TrFlie_fullName = TrFlie + suffix;
							System.out.println("TrFlie_fullName2"+TrFlie_fullName);
						}
					} else {
						TrFlie_fullName = TrFlie;
						System.out.println("TrFlie_fullName3"+TrFlie_fullName);
					}
					if (!"KEEP".equals(lfileType)) {
						fos = createOutStream(TrFlie_fullName);
					} else {
						out = createOutStreamKeep(TrFlie_fullName);
					}

					//
					int r;
					int i = 0;
					int n = 0;
					// pubAPIs.writerLog("rcutSize:"+rcutSize+"
					// lfileType:"+lfileType+" rsysType:"+rsysType);
					if (rcutSize > 0 && "CSV".equals(lfileType) && "tdh".equals(rsysType)) {
						// pubAPIs.writerLog("============1");
						while ((r = fis.read()) != -1) {
							i++;
							if (r == 34) {
								n++;
							} else if (n % 2 == 0 && r == 10 && i >= fileSize) {
								fos.write(r);
								i = 0;
								fos.close();
								m++;
								TrFlie_fullName = TrFlie + "/" + m + suffix;
								System.out.println("TrFlie_fullName4"+TrFlie_fullName);
								// 因为lfileType的值一定为CSV,所以直接用createOutStream
								fos = createOutStream(TrFlie_fullName);
								continue;
							}
							fos.write(r);
						}
						fos.close();
						fis.close();
					} else if (rcutSize > 0 && "TXT".equals(lfileType) && "tdh".equals(rsysType)) {
						// pubAPIs.writerLog("============2");
						while ((r = fis.read()) != -1) {
							i++;
							if (r == 10 && i >= fileSize) {
								fos.write(r);
								i = 0;
								fos.close();
								m++;
								TrFlie_fullName = TrFlie + "/" + m + suffix;
								System.out.println("TrFlie_fullName5"+TrFlie_fullName);
								fos = createOutStream(TrFlie_fullName);
								continue;
							}
							fos.write(r);
						}
						fos.close();
						fis.close();
					} else if (("TXT".equals(lfileType) || "CSV".equals(lfileType)) && rcutSize == 0) {
						while ((r = fis.read()) != -1) {
							fos.write(r);
						}
						fos.close();
						fis.close();
					} else if ("KEEP".equals(lfileType)) {
						int CRYPT = Integer.parseInt(!"".equals(rCRYPT) ? rCRYPT : "2");
						if (CRYPT == 2) {
							IOUtils.copyBytes(in, out, bfSize, true);
						} else {
							byte[] key = { 0x01, 0x23, 0x45, 0x68, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef,
									(byte) 0xfe, (byte) 0xdc, (byte) 0xba, (byte) 0x98, 0x76, 0x54, 0x32, 0x10 };
							String tmp[] = SMkey.split(",");
							for (int e = 0; e < tmp.length; e++) {
								key[e] = tmp[e].getBytes()[0];
							}
							int len = in.available();
							byte[] data = new byte[len];
							in.read(data);
							in.close();
							int point = 0;
							byte[] input = new byte[16];
							byte[] output = new byte[16];
							while (len >= 16) {
								input = Arrays.copyOfRange(data, point, point + 16);
								pubAPIs.sms4(input, 16, key, output, CRYPT);
								if (len == 16) {
									for (int h = 0; h < 16; h++) {
										if (output[h] != 10) {
											out.write(output[h]);
										}
									}
								} else {
									out.write(output);
								}
								len -= 16;
								point += 16;
							}
							if (len < 16 && len > 0) {
								input = Arrays.copyOfRange(data, point, point + len);
								byte[] input1 = new byte[16];
								for (int j = 0; j < 16; j++) {
									if (j < input.length) {
										input1[j] = input[j];
									} else {
										input1[j] = "\n".getBytes()[0];
									}
								}
								pubAPIs.sms4(input1, 16, key, output, CRYPT);
								out.write(output);
							}
							out.flush();
							out.close();
						}
					}
					if ("tdh".equals(rsysType)) {
						r_fileSize = Tfs_r.getContentSummary(new Path(TrFlie)).getLength();

					} else {
						r_fileSize = new File(TrFlie).length();
					}

					//
					if ("KEEP".equals(lfileType)) {
						if (l_fileSize == -1 || r_fileSize != l_fileSize) {
							throw new PubException(TlfilePath + " --> " + TrFlie + "文件大小不一致！");
						}
					}

					if (Tfs_l != null) {
						PubAPIs.getFileSystem("", "", classname,"close",Tfs_l);
					}
					if (Tfs_r != null) {
						PubAPIs.getFileSystem("", "", classname,"close",Tfs_r);
					}
				} catch (Exception e) {
					String eStr = pubAPIs.getException(e);
					pubAPIs.writerErrorLog(TlfilePath + "\n" + eStr);
					return -1;
				}
				return result;
			}

			private BufferedWriter createOutStream(String TrFlie_fullNameT) throws Exception {
				System.out.println("TrFlie_fullNameT"+TrFlie_fullNameT);
				BufferedWriter fos = null;
				OutputStream out = null;
				if ("tdh".equals(rsysType)) {
					out = Tfs_r.create(new Path(TrFlie_fullNameT), rReplication);
				} else {
					File file = new File(TrFlie_fullNameT);
					if (!file.getParentFile().exists()) {
						file.getParentFile().mkdirs();
					}
					out = new FileOutputStream(TrFlie_fullNameT);
				}
				fos = new BufferedWriter(new OutputStreamWriter(out, rEcode));
				return fos;
			}

			private BufferedReader openInputStream() throws Exception {
				BufferedReader fis = null;
				InputStream in = null;
				if ("tdh".equals(lsysType)) {
					in = Tfs_l.open(new Path(TlfilePath));
				} else {
					in = new FileInputStream(TlfilePath);
				}
				fis = new BufferedReader(new InputStreamReader(in, lEcode));
				return fis;
			}

			private OutputStream createOutStreamKeep(String TrFlie_fullNameT) throws Exception {
				OutputStream out = null;
				if ("tdh".equals(rsysType)) {
					out = Tfs_r.create(new Path(TrFlie_fullNameT), rReplication);
				} else {
					File file = new File(TrFlie_fullNameT);
					if (!file.getParentFile().exists()) {
						file.getParentFile().mkdirs();
					}
					out = new FileOutputStream(TrFlie_fullNameT);
				}
				return out;
			}

			private InputStream openInputStreamKeep() throws Exception {
				InputStream in = null;
				if ("tdh".equals(lsysType)) {
					in = Tfs_l.open(new Path(TlfilePath));
				} else {
					in = new FileInputStream(TlfilePath);
				}
				return in;
			}
		}

		public static void main(String[] args) {
			String l_loginT = "";
			String lconf_defT = "";
			String lfilePathT = "";
			String r_loginT = "";
			String rconf_defT = "";
			String rfilePathT = "";
			String table_name="";
			if (args.length == 3) {
				if ("linuxTotdh".equals(args[0])) {
					l_loginT = "transferFiles_login_username";
					lconf_defT = "transferFiles_conf_linux";
					lfilePathT = args[1];
					r_loginT = "transferFiles_login_kerberos";
					rconf_defT = "transferFiles_conf_tdh";
					rfilePathT = args[2];
				}
				if ("tdhTolinux".equals(args[0])) {
					l_loginT = "transferFiles_login_kerberos";
					lconf_defT = "transferFiles_conf_tdh";
					lfilePathT = args[1];
					r_loginT = "transferFiles_login_username";
					rconf_defT = "transferFiles_conf_linux";
					rfilePathT = args[2];
				}
			} else if (args.length == 6) {
				l_loginT = args[0];
				lconf_defT = args[1];
				lfilePathT = args[2];
				r_loginT = args[3];
				rconf_defT = args[4];
				rfilePathT = args[5];
			} else {
				r_loginT = "dataExport_login_kerberos";
				rconf_defT = "dataExport_conf_tdh";
				rfilePathT = "D://export//use_wft//app//tdh_tools//expData//20201117\\test.ciyutest\\data\\";
				l_loginT = "dataExport_login_kerberos";
				lconf_defT = "dataExport_conf_tdh";
				lfilePathT = "/export/crm/ciyutest/";
				table_name ="ciyutest.txt";
				
			}
			try {
				test transferFiles = new test("dataHandle");
				transferFiles.transferCatalogFromTDH(l_loginT, lconf_defT, lfilePathT,rfilePathT);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}


