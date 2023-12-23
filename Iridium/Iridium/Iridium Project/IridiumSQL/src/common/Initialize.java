package common;
 
import java.io.File;
import java.io.RandomAccessFile;
 
public class Initialize {
 
    private static RandomAccessFile systemTableFile;
    private static RandomAccessFile systemColumnFile;
    public static final int pageSize = 512;
 
   
    public static void InitializeMetaData() {
        File baseDir = new File(Constants.DEFAULT_DIRNAME);
        if(!baseDir.exists()) {
            File catalogDir = new File(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME);
            if(!catalogDir.exists()) {
               catalogDir.mkdirs();
             }
 
            boolean systemTablePresence = false;
            String metaColumns = "davisbase_columns.tbl";
            String metaTables = "davisbase_tables.tbl";
 
            String[] tableList = catalogDir.list();
 
            for(int i=0;i<tableList.length;i++)
            {
                if(tableList[i].equals(metaColumns))
                {
                    systemTablePresence = true;
                }
            }
 
            if(!systemTablePresence)
            {
                System.out.println("Tables not present");
                System.out.println("Table " + metaColumns + " does not exist");
                System.out.println("Creating " +  metaColumns);
                initializeDataStore(metaTables,metaColumns);
            }
 
            systemTablePresence = false;
 
            for(int i=0;i<tableList.length;i++)
            {
                if(tableList[i].equals(metaTables))
                {
                    systemTablePresence = true;
                }
            }
 
            if(!systemTablePresence)
            {
                System.out.println("Tables not present");
                System.out.println("Table " + metaTables + " does not exist");
                System.out.println("Creating " +  metaTables);
                initializeDataStore(metaTables,metaColumns);
            }
 
        }
 
    }
 
    public static void initializeDataStore(String metaTables,String metaColumns)
    {
        try
        {
            File catalogDir = new File(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME);
            String[] oldFiles =  catalogDir.list();
 
            for (int i=0; i<oldFiles.length; i++)
            {
                File anOldFile = new File(catalogDir, oldFiles[i]);
                anOldFile.delete();
            }
        }
        catch (SecurityException se)
        {
            System.out.println("Error in deleting old files :"+se);
           
        }
 
        try
        {
           
            systemTableFile = new RandomAccessFile(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME + "/" + metaTables, "rw");
            systemTableFile.setLength(pageSize);
            systemTableFile.seek(0);
            systemTableFile.write(0x0D);
            systemTableFile.write(0x02);
            int[] offset=new int[2];
            int size1=24;
            int size2=25;
            offset[0]=pageSize-size1;
            offset[1]=offset[0]-size2;
            systemTableFile.writeShort(offset[1]);
            systemTableFile.writeInt(0);
            systemTableFile.writeInt(10);
            systemTableFile.writeShort(offset[1]);
            systemTableFile.writeShort(offset[0]);
            systemTableFile.seek(offset[0]);
            systemTableFile.writeShort(20);
            systemTableFile.writeInt(1);
            systemTableFile.writeByte(1);
            systemTableFile.writeByte(28);
            systemTableFile.writeBytes("davisbase_tables");
            systemTableFile.seek(offset[1]);
            systemTableFile.writeShort(21);
            systemTableFile.writeInt(2);
            systemTableFile.writeByte(1);
            systemTableFile.writeByte(29);
            systemTableFile.writeBytes("davisbase_columns");
        }
        catch (Exception e)
        {
            System.out.println("Unable to create the database_tables file");
            System.out.println(e);
        }
 
        try
        {
            systemColumnFile = new RandomAccessFile(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME + "/" + metaColumns, "rw");
            systemColumnFile.setLength(pageSize);
            systemColumnFile.seek(0);      
            systemColumnFile.writeByte(0x0D); 
            systemColumnFile.writeByte(0x08); 
            int[] offset=new int[10];
            offset[0]=pageSize-43;
            offset[1]=offset[0]-47;
            offset[2]=offset[1]-44;
            offset[3]=offset[2]-48;
            offset[4]=offset[3]-49;
            offset[5]=offset[4]-47;
            offset[6]=offset[5]-57;
            offset[7]=offset[6]-49;
            offset[8]=offset[7]-49;
            systemColumnFile.writeShort(offset[8]);
            systemColumnFile.writeInt(0);
            systemColumnFile.writeInt(0);
            for(int i=0;i<9;i++)
                systemColumnFile.writeShort(offset[i]);
            systemColumnFile.seek(offset[0]);
            systemColumnFile.writeShort(33);
            systemColumnFile.writeInt(1);
            systemColumnFile.writeByte(5);
            systemColumnFile.writeByte(28);
            systemColumnFile.writeByte(17);
            systemColumnFile.writeByte(15);
            systemColumnFile.writeByte(4);
            systemColumnFile.writeByte(14);
            systemColumnFile.writeBytes("davisbase_tables");
            systemColumnFile.writeBytes("rowid");
            systemColumnFile.writeBytes("INT");
            systemColumnFile.writeByte(1);
            systemColumnFile.writeBytes("NO");
            systemColumnFile.seek(offset[1]);
            systemColumnFile.writeShort(39);
            systemColumnFile.writeInt(2);
            systemColumnFile.writeByte(5);
            systemColumnFile.writeByte(28);
            systemColumnFile.writeByte(22);
            systemColumnFile.writeByte(16);
            systemColumnFile.writeByte(4);
            systemColumnFile.writeByte(14);
            systemColumnFile.writeBytes("davisbase_tables");
            systemColumnFile.writeBytes("table_name"); 
            systemColumnFile.writeBytes("TEXT");
            systemColumnFile.writeByte(2);
            systemColumnFile.writeBytes("NO");
            systemColumnFile.seek(offset[2]);
            systemColumnFile.writeShort(34);
            systemColumnFile.writeInt(3);
            systemColumnFile.writeByte(5);
            systemColumnFile.writeByte(29);
            systemColumnFile.writeByte(17);
            systemColumnFile.writeByte(15);
            systemColumnFile.writeByte(4);
            systemColumnFile.writeByte(14);
            systemColumnFile.writeBytes("davisbase_columns");
            systemColumnFile.writeBytes("rowid");
            systemColumnFile.writeBytes("INT");
            systemColumnFile.writeByte(1);
            systemColumnFile.writeBytes("NO");
            systemColumnFile.seek(offset[3]);
            systemColumnFile.writeShort(40);
            systemColumnFile.writeInt(4);
            systemColumnFile.writeByte(5);
            systemColumnFile.writeByte(29);
            systemColumnFile.writeByte(22);
            systemColumnFile.writeByte(16);
            systemColumnFile.writeByte(4);
            systemColumnFile.writeByte(14);
            systemColumnFile.writeBytes("davisbase_columns");
            systemColumnFile.writeBytes("table_name");
            systemColumnFile.writeBytes("TEXT");
            systemColumnFile.writeByte(2);
            systemColumnFile.writeBytes("NO");
            systemColumnFile.seek(offset[4]);
            systemColumnFile.writeShort(41);
            systemColumnFile.writeInt(5);
            systemColumnFile.writeByte(5);
            systemColumnFile.writeByte(29);
            systemColumnFile.writeByte(23);
            systemColumnFile.writeByte(16);
            systemColumnFile.writeByte(4);
            systemColumnFile.writeByte(14);
            systemColumnFile.writeBytes("davisbase_columns");
            systemColumnFile.writeBytes("column_name");
            systemColumnFile.writeBytes("TEXT");
            systemColumnFile.writeByte(3);
            systemColumnFile.writeBytes("NO");
            systemColumnFile.seek(offset[5]);
            systemColumnFile.writeShort(39);
            systemColumnFile.writeInt(6);
            systemColumnFile.writeByte(5);
            systemColumnFile.writeByte(29);
            systemColumnFile.writeByte(21);
            systemColumnFile.writeByte(16);
            systemColumnFile.writeByte(4);
            systemColumnFile.writeByte(14);
            systemColumnFile.writeBytes("davisbase_columns");
            systemColumnFile.writeBytes("data_type");
            systemColumnFile.writeBytes("TEXT");
            systemColumnFile.writeByte(4);
            systemColumnFile.writeBytes("NO");
            systemColumnFile.seek(offset[6]);
            systemColumnFile.writeShort(49);
            systemColumnFile.writeInt(7);
            systemColumnFile.writeByte(5);
            systemColumnFile.writeByte(29);
            systemColumnFile.writeByte(28);
            systemColumnFile.writeByte(19);
            systemColumnFile.writeByte(4);
            systemColumnFile.writeByte(14);
            systemColumnFile.writeBytes("davisbase_columns");
            systemColumnFile.writeBytes("ordinal_position");
            systemColumnFile.writeBytes("TINYINT");
            systemColumnFile.writeByte(5);
            systemColumnFile.writeBytes("NO");
            systemColumnFile.seek(offset[7]);
            systemColumnFile.writeShort(41);
            systemColumnFile.writeInt(8);
            systemColumnFile.writeByte(5);
            systemColumnFile.writeByte(29);
            systemColumnFile.writeByte(23);
            systemColumnFile.writeByte(16);
            systemColumnFile.writeByte(4);
            systemColumnFile.writeByte(14);
            systemColumnFile.writeBytes("davisbase_columns");
            systemColumnFile.writeBytes("is_nullable");
            systemColumnFile.writeBytes("TEXT");
            systemColumnFile.writeByte(6);
            systemColumnFile.writeBytes("NO");
        }
        catch (Exception e)
        {
            System.out.println("Unable to create the database_columns file");
            System.out.println(e);
        }
    }
}