package com.youku.statis.D20120912;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.WritableComparable;

public class Ipad_Statis_Request implements WritableComparable<Ipad_Statis_Request> {
	// pattern
	//ƥ��������Ϣ�Ĵ�
	//private static final String realLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\" ([\\d]+) ([\\d]+)";
	
	//���ڻ�û�д������Ϣ��
	private static final String realLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\".*";
	// http
	private String ip;
	private String date;
	private String method;
	private String uri;
	private String response_code;
	private String content_length;
	private String request_time;
	private String user_agent;
	private String request_args;
	private String request_body;

	// args
	private String pid;
	private String guid;
	
	private boolean vv_statis = false;

	// labels
	private static final String pid_label = "pid";
	private static final String guid_label = "guid";

	public Ipad_Statis_Request() {
	}

	public Ipad_Statis_Request(String line) {
		try {
			Pattern realP = Pattern.compile(realLogEntryPattern);
			Matcher realMatcher = realP.matcher(line);
			if (realMatcher.matches()) {
				ip = realMatcher.group(1);
				date = realMatcher.group(2);
				method = realMatcher.group(3);
				uri = realMatcher.group(4);
				request_args = realMatcher.group(5);
				request_body = realMatcher.group(6);
				response_code = realMatcher.group(7);
				content_length = realMatcher.group(8);
				request_time = realMatcher.group(9);
				user_agent = realMatcher.group(10);
				if (user_agent == null) {
					user_agent = "";
				} else {
					try{
						user_agent = URLDecoder.decode(user_agent, "UTF-8");
					}catch(UnsupportedEncodingException une){
						une.printStackTrace();
					}
				}
				
				request_args = request_args+"&"+request_body;
				String[] args = request_args.split("&");
				Map<String, String> map = new HashMap<String, String>();
				for (String arg : args) {
					if (arg==null || arg.equals("=")){
						continue;
					}
					String[] key_value = arg.split("=");
					String key = key_value[0];
					String value = key_value.length == 2 ? key_value[1] : "";
					map.put(key, value);
				}
				pid = (String) map.get(pid_label);
				if (pid == null) {
					pid = "";
				}
				
				guid = (String)map.get(guid_label);
				if (guid == null) {
					guid = "";
				}
				
				if (uri.contains("statis/vv") && request_args.contains("type=begin")){
					vv_statis = true;
				}
			} else {
				//System.out.println("line:"+line);
			}
		} catch (Exception e) {
			//System.out.println("exception; line:"+line);
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "User [ip=" + ip + ", date=" + date + ", method=" + method
				+ ", uri=" + uri + ", response_code=" + response_code
				+ ", content_length=" + content_length + ", request_time="
				+ request_time + ", user_agent=" + user_agent + ", pid=" + pid
				+ ", guid=" + guid + ", vv_statis=" + vv_statis + "]";
	}

	public String getRequest_args() {
		return request_args;
	}

	public String getIp() {
		return ip;
	}

	public String getDate() {
		return date;
	}

	public String getMethod() {
		return method;
	}

	public String getUri() {
		return uri;
	}

	public String getResponse_code() {
		return response_code;
	}

	public String getContent_length() {
		return content_length;
	}

	public String getRequest_time() {
		return request_time;
	}

	public String getUser_agent() {
		return user_agent;
	}

	public String getPid() {
		return pid;
	}

	public String getGuid() {
		return guid;
	}

	public boolean isVv_statis() {
		return vv_statis;
	}

	public void setVv_statis(boolean vv_statis) {
		this.vv_statis = vv_statis;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(ip);
		out.writeUTF(date);
		out.writeUTF(method);
		out.writeUTF(uri);
		out.writeUTF(response_code);
		out.writeUTF(content_length);
		out.writeUTF(request_time);
		out.writeUTF(user_agent);
		out.writeUTF(request_args);

		// args
		out.writeUTF(pid);
		out.writeUTF(guid);
		out.writeBoolean(vv_statis);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ip = in.readUTF();
		date = in.readUTF();
		method = in.readUTF();
		uri = in.readUTF();
		response_code = in.readUTF();
		content_length = in.readUTF();
		request_time = in.readUTF();
		user_agent = in.readUTF();
		request_args = in.readUTF();

		// args = // args
		pid = in.readUTF();
		guid = in.readUTF();
		vv_statis = in.readBoolean();

	}

	@Override
	public int compareTo(Ipad_Statis_Request r) {
		if (r == null) {
			return 0;
		}
		
		String pid1 = pid;
		String pid2 = r.getPid();
		if (pid1.equals(pid2)) {
			return 1;
		} else {
			return -1;
		}
	}

}