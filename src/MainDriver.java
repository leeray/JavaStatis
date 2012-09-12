import org.apache.hadoop.util.ProgramDriver;

import com.youku.statis.D20120903.Ipad_Statis;
import com.youku.statis.D20120903.VV_Statis;
import com.youku.statis.D20120912.Ipad_NU_Track_Statis;

public class MainDriver {

	public static void main(String[] args) {
		int exitCode = -1;
		ProgramDriver pd = new ProgramDriver();
		try {
			pd.addClass("vvstatis", VV_Statis.class, "vv statis");
			
			pd.addClass("ipadstatis", Ipad_Statis.class, "ipad statis");

			pd.addClass("vvStatisComplete", com.youku.statis.D20120911.VV_Statis.class, "vv complete statis");

			pd.addClass("ipadtrack", Ipad_NU_Track_Statis.class, "ipad nu Track statis.");
			
			pd.driver(args);

			// Success
			exitCode = 0;
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

}
