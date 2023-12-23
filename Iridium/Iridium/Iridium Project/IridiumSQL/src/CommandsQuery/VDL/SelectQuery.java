package CommandsQuery.VDL;

import java.io.File;
import CommandsQuery.InputQuery;
import OutputHandler.Output;
import CommandsQuery.QueryHandler;
import common.Constants;
import common.UtilsTable;

public class SelectQuery implements InputQuery{

    public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
    String query = "";

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

   
    //TODO:@NEEL UPDATE THIS ALL FUNCTIONS
    public SelectQuery(String query)
    {
        this.query = query;
    }

    @Override
    public Output RunQuery() {
        //System.out.println("INSERTED");
        System.out.println(this.query);
        System.out.println("-----------------");
        String[] tempSplit = query.split("where");
        String[] partitionFrom = tempSplit[0].split("from");
        String tableName = partitionFrom[1].trim();
        String columns = partitionFrom[0].replace("select", "").trim();
        String[] comparators;
        
        String[] columnsList = new String[0];
        if(tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns"))
        {
            if (columns.contains("*")) 
            {
                columnsList = new String[1];
                columnsList[0] = "*";
            } 
            else 
            {
                columnsList = columns.split(",");
                for (int i = 0; i < columnsList.length; i++)
                    columnsList[i] = columnsList[i].trim();
            }
            if (tempSplit.length > 1) 
            {
                String filter = tempSplit[1].trim();
                comparators = parserEquation(filter);
            } else {
                comparators = new String[0];
            }
            UtilsTable.selectQuery(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME + "/" + tableName + ".tbl", tableName, columnsList, comparators);
            System.out.println();
            return new Output(1);
        }
        
        /*else if(tableName.equals("davisbase_columns"))
        {
            if (columns.contains("*")) 
            {
                columnsList = new String[1];
                columnsList[0] = "*";
            } else 
            {
                columnsList = columns.split(",");
                for (int i = 0; i < columnsList.length; i++)
                    columnsList[i] = columnsList[i].trim();
            }
            if (tempSplit.length > 1) {
                String whereCondition = tempSplit[1].trim();
                comparators = parserEquation(whereCondition);
            } else {
                comparators = new String[0];
            }
            UtilsTable.select("data\\catalog\\davisbase_columns.tbl", tableName, columnsList, comparators);
            System.out.println();
        }*/

        else
        {
            if(!tableExist(tableName)) 
            {
                System.out.println("Table " + tableName + " doesn't exist.");
                System.out.println();
                return new Output(0);
            }

            if (tempSplit.length > 1) 
            {
                String filter = tempSplit[1].trim();
                comparators = parserEquation(filter);
            } 
            else
            {
                comparators = new String[0];
            }

            if (columns.contains("*")) 
            {
                columnsList = new String[1];
                columnsList[0] = "*";
            }
            else
            {
                columnsList = columns.split(",");
                for (int i = 0; i < columnsList.length; i++)
                    columnsList[i] = columnsList[i].trim();
            }
        
            UtilsTable.selectQuery(Constants.DEFAULT_DIRNAME + "/" + QueryHandler.ActiveDBName + "/" + tableName + "/" + tableName + ".tbl", tableName, columnsList, comparators);
            System.out.println();
            return new Output(1);
        }
    }


     @Override
    public boolean CheckQueryisValid() 
    {
        return true;
    }
}
