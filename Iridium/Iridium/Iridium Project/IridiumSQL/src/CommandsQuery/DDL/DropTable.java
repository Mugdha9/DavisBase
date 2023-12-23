package CommandsQuery.DDL;

import java.io.File;
import java.util.ArrayList;

import CommandsQuery.InputQuery;
import CommandsQuery.QueryHandler;
import OutputHandler.Output;
import common.Constants;
import common.databaseCommon;
import common.UtilsTable;
import CommandsQuery.QueryHandler;

public class DropTable implements InputQuery
{
    ArrayList<String> queryTokens = null;

    public DropTable(ArrayList<String> queryTokens) {
        this.queryTokens = queryTokens;
    }

	public static boolean tableExist(String table) {
		boolean table_check = false;
		try {
			File user_tables = new File(Constants.DEFAULT_DIRNAME + "/" + QueryHandler.ActiveDBName);
			
			String[] tableList;
			tableList = user_tables.list();
			for (int i = 0; i < tableList.length; i++) {
				if (tableList[i].equals(table))
					return true;
			}
		} catch (SecurityException se) {
			System.out.println("Unable to check if data exists" + se);
		}

		return table_check;
	}

    @Override
    public Output RunQuery() {
        if(this.queryTokens.get(1).equals("table")){
            String dropTable = this.queryTokens.get(2);
            if (!tableExist(dropTable)) 
            {
                System.out.println("Table " + dropTable + " does not exist.");
                System.out.println();
                return new Output(0);
            }
            UtilsTable.drop(dropTable,QueryHandler.ActiveDBName);
            System.out.println("Table "+dropTable+" dropped successfully.");
            }
            else if(this.queryTokens.get(1).equals("database"))
            {
                String dropDB = this.queryTokens.get(2);
                if (!UtilsTable.checkDB(dropDB)) 
                {
                    System.out.println("Database " + dropDB + " does not exist.");
                    System.out.println();
                    return new Output(0);
                }
                UtilsTable.dropDB(dropDB);
                System.out.println("Database "+dropDB+" dropped successfully.");
            }
            else
            {
                System.out.println("Incorrect input");
                System.out.println(QueryHandler.USE_HELP_MESSAGE);
            }
            System.out.println();
            return new Output(1);
    }


     @Override
    public boolean CheckQueryisValid() 
    {
        return true;
    }
    
}
