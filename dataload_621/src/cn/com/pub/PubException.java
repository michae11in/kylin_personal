package cn.com.pub;

public class PubException extends Exception{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

			// 无参构造方法
			public PubException() {
				super();
			}

			// 有参的构造方法
			public PubException(String message) {
				super(message);
			}

			// 用指定的详细信息和原因构造一个新的异常
			public PubException(String message, Throwable cause) {
				super(message, cause);
			}

			// 用指定原因构造一个新的异常
			public PubException(Throwable cause) {
				super(cause);
			}
}
