public class Settings {
	static String prompt = "Iridiumsql> ";
	static String version = "v1.2";
	static String copyright = "@2023 Team Iridium";
	static boolean isExit = false;
	static int pageSize = 512;

	
	public static boolean isExit() {
		return isExit;
	}

	public static void setExit(boolean e) {
		isExit = e;
	}
	
	public static String getPrompt() {
		return prompt;
	}

	public static void setPrompt(String s) {
		prompt = s;
	}

	public static String getVersion() {
		return version;
	}

	public static void setVersion(String version) {
		Settings.version = version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void setCopyright(String copyright) {
		Settings.copyright = copyright;
	}

	public static int getPageSize() {
		return pageSize;
	}

	public static void setPageSize(int pageSize) {
		Settings.pageSize = pageSize;
	}
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
}