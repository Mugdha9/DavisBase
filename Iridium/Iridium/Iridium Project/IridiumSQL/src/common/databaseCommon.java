package common;

import java.io.File;

public class databaseCommon 
{
    public static String getDatabasePath(String databaseName) 
    {
        return Constants.DEFAULT_DIRNAME + "/" + databaseName;
    }

    public static void deleteDirectory(File file)
    {
        for (File subfile : file.listFiles()) {
 
            if (subfile.isDirectory()) {
                deleteDirectory(subfile);
            }
            subfile.delete();
        }
    }


}
