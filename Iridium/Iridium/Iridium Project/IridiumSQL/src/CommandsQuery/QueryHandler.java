package CommandsQuery;

import java.util.ArrayList;

import CommandsQuery.DDL.*;
import CommandsQuery.VDL.*;
import CommandsQuery.DML.*;
import CommandsQuery.Parsers.DatatypeList;
import CommandsQuery.Parsers.QueryColumnParser;
import OutputHandler.Output;
import common.Constants;

public class QueryHandler 
{
    //ACTIVE DB
    public static String ActiveDBName = "";
    public static final String USE_HELP_MESSAGE = "\nType 'help;' to display supported commands.";

    public static InputQuery CreateDatabaseQueryHandler(String DBName) 
    {
        return new CreateDatabase(DBName);
    }

    public static InputQuery UseDatabaseQueryHandler(String DBName) 
    {
        return new UseDatabase(DBName);
    }

    public static InputQuery ShowDatabaseQueryHandler() 
    {
        return new ShowDatabase();
    }
    public static InputQuery ShowTablesQueryHandler()
    {
        return new ShowTables(QueryHandler.ActiveDBName);
    }

    public static InputQuery DropDatabaseQueryHandler(ArrayList<String> commandTokens) 
    {
        return new DropTable(commandTokens);
    }

    public static InputQuery DropTableQueryHandler(ArrayList<String> commandTokens) 
    {
        return new DropTable(commandTokens);
    }
    //TODO:@NEEL UPDATE THIS ALL FUNCTIONS
    public static InputQuery SelectQueryHandler(String commandTokens)
    {
        return new SelectQuery(commandTokens);
    }
    public static InputQuery InsertQueryHandler(ArrayList<String> commandTokens)
    {
        return new InsertQuery(commandTokens);
    }
    public static InputQuery UpdateQueryHandler(ArrayList<String> commandTokens)
    {
        return new UpdateQuery(commandTokens);
    }
    public static InputQuery DeleteQueryHandler(String commandTokens)
    {
        return new DeleteQuery(commandTokens);
    }
    //TODO:@NEEL UPDATE THIS ALL FUNCTIONS

    public static InputQuery CreateTableQueryHandler(ArrayList<String> commandTokens) 
    {
        // if(QueryHandler.ActiveDBName.equals("")){
        //     System.out.println("No database selected");
        //     return null;
        // }
        // InputQuery query;
        // boolean hasPrimaryKey = false;
        // ArrayList<QueryColumnParser> columnsAryList = new ArrayList<>();
        // String[] columnsList = columnsStr.split(",");

        // for(String columnEntry : columnsList){
        //     QueryColumnParser column = QueryColumnParser.AddColumn(columnEntry.trim());
        //     if(column == null) return null;
        //     columnsAryList.add(column);
        // }

        // for(int Cidx = 0;Cidx< columnsList.length; Cidx++)
        // {
        //     if (columnsList[Cidx].toLowerCase().endsWith(Constants.PRIMARY_KEY_STR)) {
        //         if (Cidx == 0) {
        //             if (columnsAryList.get(Cidx).Ctype == DatatypeList.INT) {
        //                 hasPrimaryKey = true;
        //             } else {
        //                 System.out.println("PRIMARY KEY has to have INT datatype");
        //                 return null;
        //             }
        //         }
        //         else {
        //             System.out.println("Only first column should be PRIMARY KEY and has to have INT datatype.");
        //             return null;
        //         }

        //     }
        // }
        // query = new CreateTable(QueryHandler.ActiveDBName, tableFileName, columnsAryList, hasPrimaryKey);
        // return query;
        return new CreateTable(commandTokens);
    }

    public static void RunQuery(InputQuery query) {
        
        if(query!= null && query.CheckQueryisValid()){
            Output outputResult = query.RunQuery();
            if(outputResult != null){
                outputResult.Display();
            }
        }
    }
}