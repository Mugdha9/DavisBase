package common;
import InputOutput.IOFileHandler;


public class DatabaseHandler 
{
    private static DatabaseHandler databaseHandler = null;

    public static DatabaseHandler getDatabaseHandler() {
        if(databaseHandler == null) {
            return new DatabaseHandler();
        }
        return databaseHandler;
    }

    private IOFileHandler Handler;

    private DatabaseHandler() {
        Handler = new IOFileHandler();
    }
    public boolean databaseExists(String databaseName) {

        if (databaseName == null || databaseName.length() == 0) {
            return false;
        }

        return new IOFileHandler().databaseExists(databaseName);
    }
}
