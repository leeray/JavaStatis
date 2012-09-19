package com.youku.statis.D20120914;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.WritableComparable;

public class Video_Upload_Statis_Request implements WritableComparable<Video_Upload_Statis_Request> {
	private static final String realLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\" (.*) (.*)";
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
	private String city;
	private String prov;
	private String ver;
	
	private boolean vv_statis = false;
	private String datestr;

	// labels
	private static final String pid_label = "pid";
	private static final String guid_label = "guid";
	private static final String ver_label = "ver";

	public Video_Upload_Statis_Request() {
	}

	public Video_Upload_Statis_Request(String line) {
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
				
				prov = realMatcher.group(11);
				city = realMatcher.group(12);
				
				
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
				
				ver = (String)map.get(ver_label);
				if (ver == null || ver.equals("")){
					ver = "N/A";
				}else{
					ver = ver.replaceAll("[^0-9.]", "");
					if (ver == null || ver.equals("")){
						ver = "N/A";
					}
				}
				
				if (prov == null){
					prov = "";
				}
				
				if (city == null){
					city = "";
				}
				
				datestr = "";
				if(date!=null){
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+08:00"); 
					SimpleDateFormat datestrFormat = new SimpleDateFormat("yyyyMMdd");
					try {    
				        Date date1 = dateFormat.parse(date);   
				        datestr = datestrFormat.format(date1);
					} catch (Exception ex) {  
						
					} 
				}
				
			} else {
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "User [ip=" + ip + ", date=" + date + ", method=" + method
				+ ", uri=" + uri + ", response_code=" + response_code
				+ ", content_length=" + content_length + ", request_time="
				+ request_time + ", user_agent=" + user_agent + ", pid=" + pid
				+ ", guid=" + guid + ", prov=" + prov + ", city=" + city + ", ver=" + ver + "]";
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

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getProv() {
		return prov;
	}

	public void setProv(String prov) {
		this.prov = prov;
	}

	public String getVer() {
		return ver;
	}

	public void setVer(String ver) {
		this.ver = ver;
	}

	public String getDatestr() {
		return datestr;
	}

	public void setDatestr(String datestr) {
		this.datestr = datestr;
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
		//out.writeUTF(user_agent);
		out.writeUTF(request_args);

		// args
		out.writeUTF(pid);
		out.writeUTF(guid);
		out.writeUTF(prov);
		out.writeUTF(city);
		out.writeUTF(ver);
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
		//user_agent = in.readUTF();
		request_args = in.readUTF();

		// args = // args
		pid = in.readUTF();
		guid = in.readUTF();
		city = in.readUTF();
		prov = in.readUTF();
		
		ver = in.readUTF();

	}

	@Override
	public int compareTo(Video_Upload_Statis_Request r) {
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