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

public class Video_Upload_Other_Statis_Request implements WritableComparable<Video_Upload_Other_Statis_Request> {

	private String date;
	private String pid;
	private String city;
	private String prov;
	private String ver;
	
	private String datestr;

	public Video_Upload_Other_Statis_Request() {
	}

	public Video_Upload_Other_Statis_Request(String line) {
		try {
			String arg[] = line.split(" ");
			
			if (arg == null || arg.length != 5){
				date = "";
				pid = "";
				ver = "";
				prov = "";
				city = "";
			}else{
				date = arg[0];
				pid = arg[1];
				ver = arg[2];
				prov = arg[3];
				city = arg[4];
				
				if (date == null){
					date = "";
				}
				
				if (pid == null){
					pid = "";
				}
				
				if (ver == null){
					ver = "";
				}
				
				if (prov == null){
					prov = "";
				}
				
				if (city == null){
					city = "";
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "User [date=" + date + ", pid=" + pid + ", ver=" + ver
				+ ", prov=" + prov + ", city=" + city + "]";
	}

	public String getDate() {
		return date;
	}

	public String getPid() {
		return pid;
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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
		out.writeUTF(pid);
		out.writeUTF(prov);
		out.writeUTF(city);
		out.writeUTF(ver);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date = in.readUTF();
		pid = in.readUTF();
		city = in.readUTF();
		prov = in.readUTF();
		ver = in.readUTF();
	}

	@Override
	public int compareTo(Video_Upload_Other_Statis_Request r) {
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