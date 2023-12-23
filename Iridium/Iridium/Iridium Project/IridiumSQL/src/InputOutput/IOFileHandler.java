package InputOutput;

import java.io.File;
import common.databaseCommon;

public class IOFileHandler 
{
    public boolean databaseExists(String databaseName) {
        File databaseDir = new File(databaseCommon.getDatabasePath(databaseName));
        System.out.println("databaseDir : " + databaseDir.exists());
        return  databaseDir.exists();
    }
}
