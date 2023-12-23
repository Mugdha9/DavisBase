package CommandsQuery.DDL;


import java.io.File;

import CommandsQuery.InputQuery;
import CommandsQuery.QueryHandler;
import OutputHandler.Output;
import common.databaseCommon;

public class DropDatabase implements InputQuery{
    public String DBName;

    public DropDatabase(String DBName) {
        this.DBName = DBName;
    }

    @Override
    public Output RunQuery() {
        
        File databaseDir = new File(databaseCommon.getDatabasePath(this.DBName));
        System.out.println(DBName);
        databaseCommon.deleteDirectory(databaseDir);
        databaseDir.delete();

        if(QueryHandler.ActiveDBName == this.DBName){
            QueryHandler.ActiveDBName = "";
        }

        return new Output(1);
    }


     @Override
    public boolean CheckQueryisValid() 
    {
        File databaseDir = new File(databaseCommon.getDatabasePath(this.DBName));
        if(!databaseDir.exists()){
            System.out.println(String.format("ERROR: Database '%s' not exists", this.DBName));
            return false;
        }
        return true;
    }
    
    
}
