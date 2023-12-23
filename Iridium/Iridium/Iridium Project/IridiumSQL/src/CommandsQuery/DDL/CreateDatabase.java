package CommandsQuery.DDL;

import java.io.File;
import CommandsQuery.InputQuery;

import OutputHandler.Output;
import common.databaseCommon;

public class CreateDatabase implements InputQuery {
    public String DBName;

    public CreateDatabase(String DBName){
        this.DBName = DBName;
    }

    @Override
    public Output RunQuery()
    {
        File database = new File(databaseCommon.getDatabasePath(this.DBName));
        boolean isCreated = database.mkdir();
        
        System.out.println("isCreated : " + isCreated + " PATH: " + databaseCommon.getDatabasePath(this.DBName));
        if(!isCreated){
            System.out.println(String.format("ERROR(200): Unable to create database '%s'", this.DBName));
            return null;
        }
        Output OutputResult = new Output(1);
        return OutputResult;
    }

    @Override
    public boolean CheckQueryisValid() {
      //  boolean databaseExists = DatabaseHandler.getDatabaseHandler().databaseExists(this.DBName);
        File databaseDir = new File(databaseCommon.getDatabasePath(this.DBName));
        if(databaseDir.exists()){
            System.out.println(String.format("ERROR: Database '%s' already exists", this.DBName));
            return false;
        }
        
        return true;
    }
}
