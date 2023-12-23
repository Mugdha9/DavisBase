import static java.lang.System.out;

import java.util.ArrayList;
import java.util.Arrays;

import CommandsQuery.InputQuery;
import CommandsQuery.QueryHandler;

public class Commands {
	public static void parseUserCommand (String userCommand) {
		
		userCommand = userCommand.replaceAll("\n", " ");    
		userCommand = userCommand.replaceAll("\r", " ");    
		userCommand = userCommand.replaceAll(",", " , ");   
		userCommand = userCommand.replaceAll("\\(", " ( "); 
		userCommand = userCommand.replaceAll("\\)", " ) "); 
		userCommand = userCommand.replaceAll("( )+", " ");  
		userCommand = userCommand.trim();
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		switch (commandTokens.get(0).toLowerCase()) {
			case "use":
				//System.out.println("Case: USE");
				parseUseDatabase(commandTokens.get(1).toLowerCase());	
				break;
			case "show":
				//System.out.println("Case: SHOW");
				if("databases".equals(commandTokens.get(1).toLowerCase()))
				{
					parseshowDatabase();
				}
				else if("tables".equals(commandTokens.get(1).toLowerCase())){
					parseshowTables();
				}
				else
				{
					System.out.println("ERROR: Unrecognized Command");
					System.out.println(QueryHandler.USE_HELP_MESSAGE);
				}
				break;
			case "select":
				//System.out.println("Case: SELECT");
				parseSelect(commandTokens);
				break;
			case "create":
				//System.out.println("Case: CREATE");
				if("table".equals(commandTokens.get(1).toLowerCase()))
					parseCreateTable(userCommand);
					
				if("database".equals(commandTokens.get(1).toLowerCase())) {
					parseCreateDatabase(commandTokens);
				}

				if("index".equals(commandTokens.get(1).toLowerCase())) {
					System.out.println("Index Created!");
				}
					
				break;
			case "insert":
				//System.out.println("Case: INSERT");
				parseInsert(commandTokens);
				break;
			case "delete":
				//System.out.println("Case: DELETE");
				parseDelete(commandTokens);
				break;
			case "update":
				//System.out.println("Case: UPDATE");
				parseUpdate(commandTokens);
				break;
			case "drop":
				System.out.println("Case: DROP");
				if("database".equals(commandTokens.get(1).toLowerCase()))
					parseDropDatabase(commandTokens);
				if("table".equals(commandTokens.get(1).toLowerCase()))
					dropTable(commandTokens);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				Settings.setExit(true);
				break;
			case "quit":
				Settings.setExit(true);
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + Settings.getVersion());
		System.out.println(Settings.getCopyright());
	}
	public static void parseCreateDatabase(ArrayList<String> commandTokens)
	{
			String DBName = commandTokens.get(2).toLowerCase();
			System.out.println(DBName);
			InputQuery query = QueryHandler.CreateDatabaseQueryHandler(DBName);
			QueryHandler.RunQuery(query);
	}
	public static void parseUseDatabase(String DBName)
	{
			//System.out.println(DBName);
			InputQuery query = QueryHandler.UseDatabaseQueryHandler(DBName);
			QueryHandler.RunQuery(query);
	}

	public static void parseshowDatabase()
	{
			InputQuery query = QueryHandler.ShowDatabaseQueryHandler();
			QueryHandler.RunQuery(query);
	}
	public static void parseshowTables()
	{
		InputQuery query = QueryHandler.ShowTablesQueryHandler();
		QueryHandler.RunQuery(query);
	}
	public static void parseCreateTable(String command) {
		
		//System.out.println("Stub: parseCreateTable method");
		System.out.println("Command: " + command);
		ArrayList<String> commandTokens = commandStringToTokenList(command);
		InputQuery query = QueryHandler.CreateTableQueryHandler(commandTokens);
		QueryHandler.RunQuery(query);
	}

	public static void parseDropDatabase(ArrayList<String> commandToken)
	{
		InputQuery query = QueryHandler.DropDatabaseQueryHandler(commandToken);
		QueryHandler.RunQuery(query);
	}
	
	public static void show(ArrayList<String> commandTokens) {
		System.out.println("Command: " + tokensToCommandString(commandTokens));
		System.out.println("Stub: This is the show method");
	}

	public static void parseInsert (ArrayList<String> commandTokens) {
		//System.out.println("Command: " + tokensToCommandString(commandTokens));
		//System.out.println("Stub: This is the insertRecord method");
		
		//TODO:@NEEL UPDATE  FUNCTIONS ARG
		InputQuery query = QueryHandler.InsertQueryHandler(commandTokens);
		QueryHandler.RunQuery(query);
	}
	
	public static void parseDelete(ArrayList<String> commandTokens) {
		//System.out.println("Command: " + tokensToCommandString(commandTokens));
		//System.out.println("Stub: This is the deleteRecord method");
		
		//TODO:@NEEL UPDATE  FUNCTIONS ARG
		InputQuery query = QueryHandler.DeleteQueryHandler(tokensToCommandString(commandTokens));
		QueryHandler.RunQuery(query);
	}
	
	public static void dropTable(ArrayList<String> commandTokens) {
		System.out.println("Command: " + tokensToCommandString(commandTokens));
		System.out.println("Stub: This is the dropTable method.");

		InputQuery query = QueryHandler.DropTableQueryHandler(commandTokens);
		QueryHandler.RunQuery(query);
	}

	public static void parseSelect(ArrayList<String> commandTokens) {
		//System.out.println("Command: " + tokensToCommandString(commandTokens));
		//System.out.println("Stub: This is the parseQuery method");

		//TODO:@NEEL UPDATE  FUNCTIONS ARG
		InputQuery query = QueryHandler.SelectQueryHandler(tokensToCommandString(commandTokens));
		QueryHandler.RunQuery(query);
	}

	public static void parseUpdate(ArrayList<String> commandTokens) {
		System.out.println("Command: " + tokensToCommandString(commandTokens));
		//System.out.println("Stub: This is the parseUpdate method");

		//TODO:@NEEL UPDATE THIS ALL FUNCTIONS
		InputQuery query = QueryHandler.UpdateQueryHandler(commandTokens);
		QueryHandler.RunQuery(query);
	}

	public static String tokensToCommandString (ArrayList<String> commandTokens) {
		String commandString = "";
		for(String token : commandTokens)
			commandString = commandString + token + " ";
		return commandString;
	}
	
	public static ArrayList<String> commandStringToTokenList (String command) {
		command.replace("\n", " ");
		command.replace("\r", " ");
		command.replace(",", " , ");
		command.replace("\\(", " ( ");
		command.replace("\\)", " ) ");
		ArrayList<String> tokenizedCommand = new ArrayList<String>(Arrays.asList(command.split(" ")));
		return tokenizedCommand;
	}

	public static void help() {
		out.println(Utils.printSeparator("*",80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		out.println("SELECT âŸ¨column_listâŸ© FROM table_name [WHERE condition];\n");
		out.println("\tDisplay table records whose optional condition");
		out.println("\tis <column_name> = <value>.\n");
		out.println("INSERT INTO (column1, column2, ...) table_name VALUES (value1, value2, ...);\n");
		out.println("\tInsert new record into the table.");
		out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("DROP TABLE table_name;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(Utils.printSeparator("*",80));
	}
	
}