package CommandsQuery.DML;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import CommandsQuery.InputQuery;
import CommandsQuery.QueryHandler;
import OutputHandler.Output;
import common.Constants;
import common.UtilsTable;

public class InsertQuery implements InputQuery{

    ArrayList<String> queryTokens = null;

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

    //TODO:@NEEL UPDATE THIS ALL FUNCTIONS

    public InsertQuery(ArrayList<String> queryTokens)
    {
        this.queryTokens = queryTokens;
    }
    @Override
    public Output RunQuery() {
        String query = tokensToCommandString(this.queryTokens);
        String tableName = this.queryTokens.get(2);
        if (!tableExist(tableName)) {
            System.out.println("Table " + tableName + " does not exist.");
            System.out.println();
            return new Output(0);
        }
        String values = query.split("values")[1].trim();
        values = values.substring(1, values.length() - 1);
        String[] valueList = values.split(",");
        for (int i = 0; i < valueList.length; i++)
            valueList[i] = valueList[i].trim();
        RandomAccessFile rFile;
        try 
        {
            rFile = new RandomAccessFile(Constants.DEFAULT_DIRNAME + "/" +QueryHandler.ActiveDBName + "/" +tableName + "/" + tableName + ".tbl", "rw");
            UtilsTable.Insert_Into(rFile,tableName,valueList);
        } 
        catch (FileNotFoundException e)
        {
            System.out.println("Error in Inserting, cannot find file");
            e.printStackTrace();
        }
        return new Output(1);
    }


     @Override
    public boolean CheckQueryisValid() 
    {
        return true;
    }
    
}
