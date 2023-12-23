package CommandsQuery.DML;
 
import java.io.File;
import java.util.ArrayList;
 
import CommandsQuery.InputQuery;
import CommandsQuery.QueryHandler;
import OutputHandler.Output;
import common.UtilsTable;
 
public class DeleteQuery implements InputQuery
{
    //TODO:@NEEL UPDATE THIS ALL FUNCTIONS
    String query = "";
 
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
 
    //TODO:@NEEL UPDATE THIS ALL FUNCTIONS
    public DeleteQuery(String query)
    {
        this.query = query;
    }
 
    @Override
    public Output RunQuery() {
 
        String[] delete_cmp = null;
        String[] delete_temp = this.query.split("where");
        String[] deleteQuery = delete_temp[0].split("from");
        String deleteTable = deleteQuery[1].trim();
        if(!tableExist(deleteTable)) {
            System.out.println("Table " + deleteTable + " doesn't exist.");
            System.out.println("Please enter the correct table name.");
            System.out.println();
            return new Output(0);
        }
       
        if (delete_temp.length > 1) {
            String filter = delete_temp[1].trim();  
            delete_cmp = parserEquation(filter);
        } else {
            delete_cmp = new String[0];
        }
        UtilsTable.delete(deleteTable, delete_cmp);
        System.out.println();
        return new Output(1);
    }
 
 
     @Override
    public boolean CheckQueryisValid()
    {
        return true;
    }
}
 