package com.youku.statis.D20120914;

/**
 * Common fields for Apache Log demo.
 */
interface LogExample {
	/** The number of fields that must be found. */
	public static final int NUM_FIELDS = 9;

	/** The real env log */
	public static final String[] realLogEntryLine = {
			"111.2.144.201 \"2012-08-10T17:53:40+08:00\" POST \"/openapi-wireless/statis/vv\" \"pid=fc34cb9fa6258940&guid=e4795607a007dbbd8648363eb62a6c31&ver=2.3.1&operator=_46002&network=WIFI&sessionid=750900DF2E15C080&id=XMzUyNDI5OTAw&play_types=local&play_codes=200&type=beginhttp://api.3g.youku.com/openapi-wireless/statis/vv?pid=fc34cb9fa6258940&guid=e4795607a007dbbd8648363eb62a6c31&ver=2.3.1&operator=_46002&network=WIFI&sessionid=A638121240B4A726&id=XMzUyNDI4NzAw&play_types=net&play_codes=-999&type=beginhttp://api.3g.youku.com/openapi-wireless/statis/vv?pid=fc34cb9fa6258940&guid=e4795607a007dbbd8648363eb62a6c31&ver=2.3.1&operator=_46002&network=WIFI&sessionid=EC8A2A17A0AA4517&id=XMzUyNDMzNzA0&play_types=local&play_codes=200&type=begin\" \"-\" 200 29 0.003 \"Youku;2.3.1;Android;2.3.5;XT316\" 1156330000 1156330300",
			"119.190.91.71 \"2012-08-10T19:55:06+08:00\" POST \"/openapi-wireless/statis/vv\" \"pid=7a4f3a9290c8d4dd&guid=614815db8df5ac4603d4806d06762797&ver=2.3.1&operator=_46001&network=WIFI&sessionid=C7BD4EC19F55CBA7&id=XMTExOTczODcy&play_types=net&play_codes=-999&type=beginhttp://api.3g.youku.com/openapi-wireless/statis/vv?pid=7a4f3a9290c8d4dd&guid=614815db8df5ac4603d4806d06762797&ver=2.3.1&operator=_46001&network=WIFI&sessionid=31436599F2C95FDC&id=XMTExOTczODcy&play_types=net&play_codes=200&type=begin\" \"-\" 200 29 0.003 \"Youku;2.3.1;Android;2.3.5;Lenovo%20A65\" 1156370000 1156370700",
			"119.39.249.42 \"2012-08-10T09:27:49+08:00\" POST \"/openapi-wireless/statis/vv\" \"pid=fc34cb9fa6258940&guid=0f391ec2a17a81526d7e83607cf082e6&ver=2.3.1&operator=_46001&sessionid=E76CC325D375E110&id=XMzEyNjkwNDA4&play_types=net&play_codes=-999&type=beginhttp://api.3g.youku.com/openapi-wireless/statis/vv?pid=fc34cb9fa6258940&guid=0f391ec2a17a81526d7e83607cf082e6&ver=2.3.1&operator=_46001&sessionid=BC9206F208334D21&id=XMzEyNjkwNDA4&play_types=local&play_codes=200&type=begin\" \"-\" 200 29 0.003 \"Youku;2.3.1;Android;2.2.2;MB525\" 1156430000 1156430100"
	};
}
