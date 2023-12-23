package CommandsQuery.DDL;

import java.io.File;
import java.util.ArrayList;


import CommandsQuery.InputQuery;
import OutputHandler.Output;
import common.databaseCommon;
public class ShowTables implements InputQuery
{
    public String DBName;

    public ShowTables(String DBName) {
        this.DBName = DBName;
    }
    @Override
    public Output RunQuery() {
        ArrayList<String> records = GetTablelist();
        System.out.println("\nTables");
        System.out.println("--------");
        for(int Idx = 0; Idx < records.size(); Idx++){
            System.out.println(records.get(Idx));
        }
        System.out.println("");
        return new Output(records.size());
    }


     @Override
    public boolean CheckQueryisValid() 
    {
        return true;
    }

    private ArrayList<String> GetTablelist(){
        ArrayList<String> records = new ArrayList<>();

        File dataFile = new File(databaseCommon.getDatabasePath(this.DBName) );
        File[] dataFileList = dataFile.listFiles();
        String TableName = "";
        System.out.println("DB_NAME: " + this.DBName);
        for(File data : dataFileList){
            TableName = data.getName().toString();
            TableName = TableName.trim();
            System.out.println("TABLE_NAME: " + TableName);
            int index = TableName.lastIndexOf('.');
            if(index > 0) {
                String extension = TableName.substring(index + 1);
                if(("tbl".equals(extension)))
                    records.add(TableName);
            }
        }
        return records;
    }
}
