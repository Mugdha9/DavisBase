package CommandsQuery.Parsers;

import common.Constants;

public class QueryColumnParser 
{
    public String Cname;
    public DatatypeList Ctype;
    public boolean isNull;

    private QueryColumnParser(String name, DatatypeList type, boolean isNull) {
        this.Cname = name;
        this.Ctype = type;
        this.isNull = isNull;
    }

    public static QueryColumnParser AddColumn(String columnStr)
    {
        boolean isNull = true;

        if(columnStr.toLowerCase().endsWith(Constants.PRIMARY_KEY_STR)){
            columnStr = columnStr.substring(0, columnStr.length() - Constants.PRIMARY_KEY_STR.length()).trim();
        }
        else if(columnStr.toLowerCase().endsWith(Constants.NOT_NULL_STR)){
            columnStr = columnStr.substring(0, columnStr.length() - Constants.NOT_NULL_STR.length()).trim();
            isNull = false;
        }

        String[] columnParts = columnStr.split(" ");
        String ClmName;
        if(columnParts.length > 2){
            System.out.println("Expected column format <name> <datatype> [PRIMARY KEY | NOT NULL]");
            return null;
        }

        if(columnParts.length > 1){
            ClmName = columnParts[0].trim();
            DatatypeList type = Get_Data_TypeEnum(columnParts[1].trim());
            if(type == null){
                System.out.println("Unrecognised data type " + columnParts[1]);
                return null;
            }

            return new QueryColumnParser(ClmName, type, isNull);
        }
        System.out.println("Expected column format <name> <datatype> [PRIMARY KEY | NOT NULL]");
        return null;
    }

    private static DatatypeList Get_Data_TypeEnum(String dataTypeStr) {
        switch(dataTypeStr){
            case "tinyint": return DatatypeList.TINYINT;
            case "smallint": return DatatypeList.SMALLINT;
            case "int": return DatatypeList.INT;
            case "bigint": return DatatypeList.BIGINT;
            case "real": return DatatypeList.REAL;
            case "double": return DatatypeList.DOUBLE;
            case "datetime": return DatatypeList.DATETIME;
            case "date": return DatatypeList.DATE;
            case "text": return DatatypeList.TEXT;
        }

        return null;
    }
}
