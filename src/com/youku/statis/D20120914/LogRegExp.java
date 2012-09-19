package com.youku.statis.D20120914;

/**
 * Parse an Apache log file with Regular Expressions
 */
public class LogRegExp implements LogExample {

	public static void main(String argv[]) {
		for (String line : realLogEntryLine) {
			Video_Upload_Statis_Request u = new Video_Upload_Statis_Request(line);
			System.out.println(u);
		}
	}

}