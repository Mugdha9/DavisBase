package CommandsQuery.DDL;

import java.io.File;

import CommandsQuery.InputQuery;
import OutputHandler.Output;

public class ShowDatabase implements InputQuery
{
    int databaseCount;
    @Override
    public Output RunQuery() {
        System.out.println("\nDatabases");
        System.out.println("----------");
        System.out.println(GetDatabaseslist());
        return new Output(databaseCount);
    }

     @Override
    public boolean CheckQueryisValid() 
    {
        return true;
    }

    private String GetDatabaseslist(){
        
        File dataFile = new File("data");
        File[] dataFileList = dataFile.listFiles();

        StringBuilder fileListString = new StringBuilder();
        for(File data : dataFileList){
            if(!data.isDirectory()) continue;
            
            fileListString.append(data.getName()).append("\n");
            databaseCount = databaseCount + 1;
        }
        String DBList = fileListString.toString();
        return DBList;
    }
}
