package CommandsQuery.DDL;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import CommandsQuery.InputQuery;
import CommandsQuery.QueryHandler;
import CommandsQuery.Parsers.QueryColumnParser;
import OutputHandler.Output;
import common.Constants;
import common.UtilsTable;

public class CreateTable implements InputQuery
{
    ArrayList<String> queryTokens = null;

    public static String tokensToCommandString (ArrayList<String> commandTokens) {
		String commandString = "";
		for(String token : commandTokens)
			commandString = commandString + token + " ";
		return commandString;
	}

    public CreateTable(ArrayList<String> queryTokens){
        this.queryTokens = queryTokens;
    }

	public static boolean tableExist(String table) 
    {
		boolean tablePresent = false;
		try 
        {
			File user_tables = new File(Constants.DEFAULT_DIRNAME + "/" + QueryHandler.ActiveDBName);
			String[] tableList;
			tableList = user_tables.list();
			for (int i = 0; i < tableList.length; i++) 
            {
				if (tableList[i].equals(table))
					return true;
			}
		} 
        catch (SecurityException e) 
        {
			System.out.println("Unable to check if table exists" + e);
		}

		return tablePresent;
	}

    @Override
    public Output RunQuery() {
        String query = tokensToCommandString(this.queryTokens);
        if(this.queryTokens.get(1).equals("table"))
        {
            String tableName = queryTokens.get(2);

            if (tableExist(tableName)) 
            {
                System.out.println("Table " + tableName + " already exists.");
                System.out.println();
                return new Output(0);
            }

            String[] createPartition = query.split(tableName);

            String columns = createPartition[1].trim();
            String[] columnList = columns.substring(1, columns.length() - 1).split(",");

            for (int i = 0; i < columnList.length; i++)
                columnList[i] = columnList[i].trim();

            UtilsTable.createTable(tableName, columnList);
        }
        /*else if(this.queryTokens.get(1).equals("database"))
        {
            String createDB = this.queryTokens.get(2);
        
            UtilsTable.createDB(createDB);
            
        }*/
        else{
            System.out.println("Unrecognized Command");
            System.out.println(QueryHandler.USE_HELP_MESSAGE);				
        }
        
        return new Output(1);
    }


     @Override
    public boolean CheckQueryisValid() 
    {
        return true;
    }



    private boolean isduplicateColumns(ArrayList<QueryColumnParser> columnArrayList) {
        HashMap<String, Integer> map = new HashMap<>();
        for (int Cidx = 0; Cidx < columnArrayList.size(); Cidx++) {
            QueryColumnParser column = columnArrayList.get(Cidx);
            if (map.containsKey(column.Cname)) {
                return true;
            }
            else {
                map.put(column.Cname, Cidx);
            }
        }

        return false;
    }
}
