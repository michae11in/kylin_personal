package cn.com.oracleTest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class CommonShellExecutor {

	private String ip = "172.20.235.177";
	private String osUsername = "oracle";
	private String password = "oracle";
	private String charset = Charset.defaultCharset().toString();
	private Connection conn;
	private static final int TIME_OUT = 1000 * 5 * 60;
	Session session = null;

	private boolean login() throws IOException {
		conn = new Connection(ip);
		conn.connect();// 建立连接
		boolean bl = conn.authenticateWithPassword(osUsername, password);
		if (bl) {
			session = conn.openSession();
			if (session != null) {
				return true;
			}
		}
		return false;
	}

	public int exec(String cmds) throws Exception {
		InputStream stdOut = null;
		InputStream stdErr = null;
		String outStr = "";
		String outErr = "";
		int ret = -1;
		if (login()) {
			session.execCommand(cmds);
			stdOut = new StreamGobbler(session.getStdout());
			outStr = processStream(stdOut, charset);
			stdErr = new StreamGobbler(session.getStderr());
			outErr = processStream(stdErr, charset);
			session.waitForCondition(ChannelCondition.EXIT_STATUS, TIME_OUT);
			System.out.println("outStr=" + outStr);
            System.out.println("outErr=" + outErr);
			ret = session.getExitStatus();
		} else {
			throw new Exception("登录远程机器失败" + ip); // 自定义异常类 实现略
		}
		return ret;
	}

	public void close() throws Exception {
		session.close();
		conn.close();
	}

	private String processStream(InputStream in, String charset) throws Exception {
		byte[] buf = new byte[1024];
		StringBuilder sb = new StringBuilder();
		while (in.read(buf) != -1) {
			sb.append(new String(buf, charset));
		}
		return sb.toString();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CommonShellExecutor commonShell = new CommonShellExecutor();
		try {
			int ret = 0;
			ret = commonShell.exec("ls");
			System.out.println(ret);
			ret = commonShell.exec("source /home/oracle/.bash_profile;sqlldr 'tdh/tdh@orcl' control='/home/oracle/ciyutest.ctl' log='/home/oracle/ciyu.log'");
			System.out.println(ret);
			commonShell.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}