package CommandsQuery.DML;

import java.io.File;
import java.util.ArrayList;
import CommandsQuery.InputQuery;
import OutputHandler.Output;
import CommandsQuery.QueryHandler;
import common.UtilsTable;

public class UpdateQuery implements InputQuery{

	//TODO:@NEEL UPDATE THIS ALL FUNCTIONS
    ArrayList<String> commandTokens = null;

	public static String[] parserEquation(String equ) 
	{
		String cmp[] = new String[3];
		String temp[] = new String[2];
		if (equ.contains("=")) {
			temp = equ.split("=");
			cmp[0] = temp[0].trim();
			cmp[1] = "=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains(">")) {
			temp = equ.split(">");
			cmp[0] = temp[0].trim();
			cmp[1] = ">";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<")) {
			temp = equ.split("<");
			cmp[0] = temp[0].trim();
			cmp[1] = "<";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains(">=")) {
			temp = equ.split(">=");
			cmp[0] = temp[0].trim();
			cmp[1] = ">=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<=")) {
			temp = equ.split("<=");
			cmp[0] = temp[0].trim();
			cmp[1] = "<=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<>")) {
			temp = equ.split("<>");
			cmp[0] = temp[0].trim();
			cmp[1] = "<>";
			cmp[2] = temp[1].trim();
		}

		return cmp;
	}

	// Check if the table exists
	public static boolean tableExist(String table) {
		boolean table_check = false;
		//table = table + ".tbl";
		try {
			File user_tables = new File("data\\"+QueryHandler.ActiveDBName);
			if (user_tables.mkdir()) {
				System.out.println("System directory 'data\\user_data' doesn't exit, Initializing user_data!");
				//Table.initializeDataStore();
			}
			String[] tableList;
			tableList = user_tables.list();
			for (int i = 0; i < tableList.length; i++) {
				if (tableList[i].equals(table))
					return true;
			}
		} catch (SecurityException se) {
			System.out.println("Unable to create data container directory" + se);
		}

		return table_check;
	}

    public static String tokensToCommandString (ArrayList<String> commandTokens) {
		String commandString = "";
		for(String token : commandTokens)
			commandString = commandString + token + " ";
		return commandString;
	}

    public UpdateQuery(ArrayList<String> commandTokens)
    {
        this.commandTokens = commandTokens;
    }

    @Override
    public Output RunQuery() {
        String[] queryTokens = new String[this.commandTokens.size()];
        for (int i = 0; i < this.commandTokens.size(); i++) {
            queryTokens[i] = this.commandTokens.get(i);
        }
        String updateTable = queryTokens[1];
        String query = tokensToCommandString(commandTokens);
        String[] partition1 = query.split("set");
        String[] partition2 = partition1[1].split("where");
        String conditions = partition2[1];
        String parameter = partition2[0];
        String[] set = parserEquation(parameter);
        String[] comparators = parserEquation(conditions);
        if (!tableExist(updateTable)) 
        {
            System.out.println("Table " + updateTable + " does not exist.");
            System.out.println();
			return new Output(0);
        }
        UtilsTable.update(updateTable, set, comparators);
        System.out.println("Table " + updateTable + " updated!");
        System.out.println();
        return new Output(1);
    }


     @Override
    public boolean CheckQueryisValid() 
    {
        return true;
    }
}
