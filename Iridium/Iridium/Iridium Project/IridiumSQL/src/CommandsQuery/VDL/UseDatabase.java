package CommandsQuery.VDL;




import java.io.File;

import CommandsQuery.InputQuery;
import CommandsQuery.QueryHandler;
import OutputHandler.Output;
import common.databaseCommon;

public class UseDatabase implements InputQuery {
    public String DBName;

    public UseDatabase(String DBName) {
        this.DBName = DBName;
    }

    @Override
    public Output RunQuery() {
        QueryHandler.ActiveDBName = this.DBName;
        System.out.println("Database changed");
        return null;
    }

    @Override
    public boolean CheckQueryisValid() {
        File databaseDir = new File(databaseCommon.getDatabasePath(this.DBName));
        if(!databaseDir.exists()){
            System.out.println("ERROR: The database "+ this.DBName + " does not exist");
            return false;
        }
        return true;
    }
}
