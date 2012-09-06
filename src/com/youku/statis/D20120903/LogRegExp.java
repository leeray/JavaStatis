package com.youku.statis.D20120903;

/**
 * Parse an Apache log file with Regular Expressions
 */
public class LogRegExp implements LogExample {

	public static void main(String argv[]) {
		for (String line : realLogEntryLine) {
			VV_Statis_Request u = new VV_Statis_Request(line);
			System.out.println(u);
		}
	}

}