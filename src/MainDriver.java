import org.apache.hadoop.util.ProgramDriver;

import com.youku.statis.D20120903.Ipad_Statis;
import com.youku.statis.D20120903.VV_Statis;

public class MainDriver {

	public static void main(String[] args) {
		int exitCode = -1;
		ProgramDriver pd = new ProgramDriver();
		try {
			pd.addClass("vvstatis", VV_Statis.class, "vv statis");
			
			pd.addClass("ipadstatis", Ipad_Statis.class, "ipad statis");

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