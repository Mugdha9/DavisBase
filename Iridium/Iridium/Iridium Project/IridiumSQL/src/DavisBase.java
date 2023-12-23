import java.util.Scanner;

import common.Initialize;

public class DavisBase {
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
    public static void main(String[] args) {
		Utils.splashScreen();

		Initialize.InitializeMetaData();
		String userCommand = ""; 

		while(!Settings.isExit()) {
			System.out.print(Settings.getPrompt());
			//remove new lines and carriage return
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			Commands.parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
	}
}