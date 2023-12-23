package common;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import CommandsQuery.QueryHandler;

class Buffer
{
	public int noOfRows;
	public HashMap<Integer, String[]> content;
	public int[] format;
	public String[] columnName;

	public Buffer()
	{
		noOfRows = 0;
		content = new HashMap<Integer, String[]>();
	}

	public void add(int rowid, String[] val)
	{
		content.put(rowid, val);
		noOfRows = noOfRows + 1;
	}

	public void updateFormat()
	{
		for(int i = 0; i < format.length; i++)
			format[i] = columnName[i].length();
		for(String[] i : content.values()){
			for(int j = 0; j < i.length; j++)
				if(format[j] < i[j].length())
					format[j] = i[j].length();
		}
	}

	public String fix(int len, String s) 
	{
		return String.format("%-"+(len+3)+"s", s);
	}

	public String line(String s,int len) 
	{
		String a = "";
		for(int i=0;i<len;i++) 
		{
			a += s;
		}
		return a;
	}

	public void display(String[] col)
	{
		if(noOfRows == 0)
		{
			System.out.println("");
		}
		else
		{
			updateFormat();
			if(col[0].equals("*"))
			{
				for(int l: format)
					System.out.print(line("-", l+3));
				System.out.println();
				for(int j = 0; j < columnName.length; j++)
					System.out.print(fix(format[j], columnName[j])+"|");
				System.out.println();
				for(int l: format)
					System.out.print(line("-", l+3));
				System.out.println();
				for(String[] i : content.values()){
					if(i[0].equals("-10000"))
						continue;
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(format[j], i[j])+"|");
					System.out.println();
				}
				System.out.println();
			}
			else
			{
				int[] control = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < columnName.length; i++)
						if(col[j].equals(columnName[i]))
							control[j] = i;
				for(int j = 0; j < control.length; j++)
					System.out.print(line("-", format[control[j]]+3));
				System.out.println();
				for(int j = 0; j < control.length; j++)
					System.out.print(fix(format[control[j]], columnName[control[j]])+"|");
				System.out.println();
				for(int j = 0; j < control.length; j++)
					System.out.print(line("-", format[control[j]]+3));
				System.out.println();
				for(String[] i : content.values())
				{
					for(int j = 0; j < control.length; j++)
						System.out.print(fix(format[control[j]], i[control[j]])+"|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}

public class UtilsTable
{
	public static final int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	private static RandomAccessFile davisbaseTablesCatalog;
	private static RandomAccessFile davisbaseColumnsCatalog;
	public static void show()
	{
		String[] cols = {"table_name"};
		String[] cmp = new String[0];
		String table = "davisbase_tables";
		
		selectQuery("data\\catalog\\"+table+".tbl",table, cols, cmp);
	}
	
	public static void showDB()
	{
		
		File f= new File("data");
		String[] listDir = f.list();
		
		for(String i:listDir){
			if(i.equals("catalog") || i.equals("user_data"))
				continue;
			System.out.println(i);
		}
		
	}

	public static void drop(String table,String db)
	{
		try{
			RandomAccessFile file = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page ++)
			{
				file.seek((page-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = UtilsPage.Get_Array_Cell(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = UtilsPage.get_Cell_Location(file, page, j);
						String[] pl = Retrieve_Payload(file, loc);
						String tb = pl[1];
						if(!tb.equals(QueryHandler.ActiveDBName+"."+table)){
							UtilsPage.Set_Offset_Cell(file, page, i, cells[j]);
							i++;
						}
					}
					UtilsPage.Set_Cell_Num(file, page, (byte)i);
				}
			}

			file = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			numPages = pages(file);
			for(int page = 1; page <= numPages; page ++){
				file.seek((page-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = UtilsPage.Get_Array_Cell(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = UtilsPage.get_Cell_Location(file, page, j);
						String[] pl = Retrieve_Payload(file, loc);
						String tb = pl[1];
						if(!tb.equals(QueryHandler.ActiveDBName+"."+table))
						{
							UtilsPage.Set_Offset_Cell(file, page, i, cells[j]);
							i++;
						}
					}
					UtilsPage.Set_Cell_Num(file, page, (byte)i);
				}
			}
			file.close();
			File dropTable = new File("data\\"+db+"\\"+table);
			String[] listFiles = dropTable.list();
			for(String f:listFiles){
				File dropFile = new File("data\\"+db+"\\"+table,f);
				dropFile.delete();
			}
			dropTable = new File("data\\"+db, table); 
			dropTable.delete();
		}
		catch(Exception e)
		{
			System.out.println("Error at drop");
			System.out.println(e);
		}

	}
	
	
	public static void dropDB(String database){
		File f= new File("data\\"+database);
		String[] listDir = f.list();
		
		for(String i:listDir){
			if(i.equals("catalog") || i.equals("user_data"))
				continue;
			drop(i,database);
		}
		File dropFile = new File("data", database); 
		dropFile.delete();
	}
	public static void createDB(String database){
		try {
			
			File db = new File("data\\"+database);
			
			if(db.exists()){
				System.out.println("Database already exists");
				return;
			}
			db.mkdir();
			QueryHandler.ActiveDBName=database;
			
			System.out.println("Database "+database+" created successfully.");
		}
		catch (SecurityException se) 
		{
			System.out.println("Unable to create catalog directory :"+se);
			
		}

	}
	
	public static boolean checkDB(String database){
		File catalog = new File("data\\"+database);
		
		if(catalog.exists()){
			QueryHandler.ActiveDBName=database;
			
			
			return true;
		}
		return false;
	}

	public static String[] Retrieve_Payload(RandomAccessFile file, long loc)
	{
		String[] payload = new String[0];
		try{
			Long tmp;
			SimpleDateFormat formater = new SimpleDateFormat (datePattern);

			// get stc
			file.seek(loc);
			int plsize = file.readShort();
			int key = file.readInt();
			int num_cols = file.readByte();
			byte[] stc = new byte[num_cols];
			int temp = file.read(stc);
			payload = new String[num_cols+1];
			payload[0] = Integer.toString(key);
			// get payLoad
			for(int i=1; i <= num_cols; i++){
				switch(stc[i-1]){
					case 0x00:  payload[i] = Integer.toString(file.readByte());
								payload[i] = "null";
								break;

					case 0x01:  payload[i] = Integer.toString(file.readShort());
								payload[i] = "null";
								break;

					case 0x02:  payload[i] = Integer.toString(file.readInt());
								payload[i] = "null";
								break;

					case 0x03:  payload[i] = Long.toString(file.readLong());
								payload[i] = "null";
								break;

					case 0x04:  payload[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  payload[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  payload[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  payload[i] = Long.toString(file.readLong());
								break;

					case 0x08:  payload[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  payload[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  tmp = file.readLong();
								Date dateTime = new Date(tmp);
								payload[i] = formater.format(dateTime);
								break;

					case 0x0B:  tmp = file.readLong();
								Date date = new Date(tmp);
								payload[i] = formater.format(date).substring(0,10);
								break;

					default:    int len = new Integer(stc[i-1]-0x0C);
								byte[] bytes = new byte[len];
								for(int j = 0; j < len; j++)
									bytes[j] = file.readByte();
								payload[i] = new String(bytes);
								break;
				}
			}

		}
		catch(Exception e)
		{
			System.out.println("Error at retrievePayload");
		}

		return payload;
	}


	public static void createTable(String table, String[] col)
	{
		try{	
			//file
			File catalog = new File("data\\"+QueryHandler.ActiveDBName+"\\"+table);
			
			catalog.mkdir();
			RandomAccessFile file = new RandomAccessFile("data\\"+QueryHandler.ActiveDBName+"\\"+table+"\\"+table+".tbl", "rw");
			file.setLength(pageSize);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
			
			// table
			file = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
			int numPages = pages(file);
			int page = 1;
			for(int p = 1; p <= numPages; p++)
			{
				int rm = UtilsPage.get_RightMost_Page(file, p);
				if(rm == 0)
					page = p;
			}
			int[] keyArray = UtilsPage.Get_Key_Array(file, page);
			int l = keyArray[0];
			for(int i = 0; i < keyArray.length; i++)
				if(l < keyArray[i])
					l = keyArray[i];
			file.close();
			String[] values = {Integer.toString(l+1), QueryHandler.ActiveDBName+"."+table};
			Insert_Into("davisbase_tables", values);

			RandomAccessFile cfile = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {};
			filter(cfile, cmp, columnName, buffer);
			l = buffer.content.size();

			for(int i = 0; i < col.length; i++){
				l = l + 1;
				String[] token = col[i].split(" ");
				String n = "YES";
				if(token.length > 2)
					n = "NO";
				String col_name = token[0];
				String dt = token[1].toUpperCase();
				String pos = Integer.toString(i+1);
				String[] v = {Integer.toString(l), QueryHandler.ActiveDBName+"."+table, col_name, dt, pos, n};
				Insert_Into("davisbase_columns", v);
			}
			cfile.close();
			file.close();
		}catch(Exception e){
			System.out.println("Error at createTable");
			e.printStackTrace();
		}
	}

	public static void update(String table, String[] set, String[] cmp)
	{
		try{
			List<Integer> key = new ArrayList<Integer>();
		
			RandomAccessFile file = new RandomAccessFile("data\\"+QueryHandler.ActiveDBName+"\\"+table+"\\"+table+".tbl", "rw");
			
			Buffer buffer = new Buffer();
			String[] columnName = Get_Column_Name(table);
			String[] type = Get_Data_Type(table);
			filter(file, cmp, columnName, type, buffer);
			
			for(String[] i : buffer.content.values()){
				
				for(int j = 0; j < i.length; j++)
					if(buffer.columnName[j].equals(cmp[0]) && i[j].equals(cmp[2])){
						key.add(Integer.parseInt(i[0]));							
						break;
					}
			
			}
				
			
			for(int indKey:key){
			
				int numPages = pages(file);
				int page = 1;
	
				for(int p = 1; p <= numPages; p++)
					if(UtilsPage.Has_Key(file, p, indKey)){
						page = p;
					}
				int[] array = UtilsPage.Get_Key_Array(file, page);
				int id = 0;
				for(int i = 0; i < array.length; i++)
					if(array[i] == indKey)
						id = i;
				int offset = UtilsPage.Get_Offset_Cell(file, page, id);
				long loc = UtilsPage.get_Cell_Location(file, page, id);
				String[] array_s = Get_Column_Name(table);
				int num_cols = array_s.length - 1;
				String[] values = Retrieve_Payload(file, loc);
	
	
				
				for(int i=0; i < type.length; i++)
					if(type[i].equals("DATE") || type[i].equals("DATETIME"))
						values[i] = "'"+values[i]+"'";
	
	
				// update value on a column
				for(int i = 0; i < array_s.length; i++)
					if(array_s[i].equals(set[0]))
						id = i;
				values[id] = set[2];
	
				// check null value violation
				String[] nullable = Get_Nullable(table);
	
				for(int i = 0; i < nullable.length; i++){
					if(values[i].equals("null") && nullable[i].equals("NO")){
						System.out.println("NULL value constraint violation");
						System.out.println();
						return;
					}
				}
	
				byte[] stc = new byte[array_s.length-1];
				int plsize = Calculate_Payload(table, values, stc);
				UtilsPage.Update_Leaf_Cell(file, page, offset, plsize, indKey, stc, values,table);
			}
			file.close();

		}catch(Exception e){
			System.out.println("Error at update");
			System.out.println(e);
		}
	}

	public static void Insert_Into(RandomAccessFile file, String table, String[] values)
	{
		String[] dataType = Get_Data_Type(table);
		String[] nullable = Get_Nullable(table);

		for(int i = 0; i < nullable.length; i++)
			if(values[i].equals("null") && nullable[i].equals("NO")){
				System.out.println("NULL value constraint violated");
				System.out.println();
				return;
			}


		int key = new Integer(values[0]);
		int page = Search_Key(file, key);
		if(page != 0)
			if(UtilsPage.Has_Key(file, page, key))
			{
				System.out.println("Uniqueness constraint violated");
				System.out.println();
				return;
			}
		if(page == 0)
			page = 1;


		byte[] shortCode = new byte[dataType.length-1];
		short payLoadSize = (short) Calculate_Payload(table, values, shortCode);
		int cellSize = payLoadSize + 6;
		int offset = UtilsPage.Leaf_Space_Present(file, page, cellSize);

		if(offset != -1)
		{
			UtilsPage.Insert_Leaf_Cell(file, page, offset, payLoadSize, key, shortCode, values,table);
		}
		else
		{
			UtilsPage.Split_Leaf(file, page);
			Insert_Into(file, table, values);
		}
	}

	public static void Insert_Into(String table, String[] values)
	{
		try
		{
			RandomAccessFile file = new RandomAccessFile("data\\catalog\\"+table+".tbl", "rw");
			Insert_Into(file, table, values);
			file.close();

		}
		catch(Exception e)
		{
			System.out.println("Error in inserting the data");
			e.printStackTrace();
		}
	}

	public static int Calculate_Payload(String tableName, String[] value, byte[] shortCodes)
	{
		String[] dataType = Get_Data_Type(tableName);
		int size = 1;
		size = size + dataType.length - 1;
		for(int i = 1; i < dataType.length; i++)
		{
			byte temp = Short_Code(value[i], dataType[i]);
			shortCodes[i - 1] = temp;
			size = size + Feild_Length(temp);
		}
		return size;
	}

	public static short Feild_Length(byte shortCodes)
	{
		switch(shortCodes)
		{
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(shortCodes - 0x0C);
		}
	}

	public static byte Short_Code(String value, String dataType)
	{
		if(value.equals("null"))
		{
			switch(dataType)
			{
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}
		else
		{
			switch(dataType)
			{
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(value.length()+0x0C);
				default:			return 0x00;
			}
		}
	}

	public static int Search_Key(RandomAccessFile file, int key)
	{
		int value = 1;
		try
		{
			int numOfPages = pages(file);
			for(int page = 1; page <= numOfPages; page++)
			{
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D)
				{
					int[] keys = UtilsPage.Get_Key_Array(file, page);
					if(keys.length == 0)
						return 0;
					int rightMost = UtilsPage.get_RightMost_Page(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1])
					{
						return page;
					}
					else if(rightMost == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Unable to search key");
			System.out.println(e);
		}

		return value;
	}


	public static String[] Get_Data_Type(String table)
	{
		String[] dataType = new String[0];
		try
		{
			RandomAccessFile file = new RandomAccessFile(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME + "/" + "davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
				table = QueryHandler.ActiveDBName+"."+table;
			String[] comparator = {"table_name","=",table};
			filter(file, comparator, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[3]);
			}
			dataType = array.toArray(new String[array.size()]);
			file.close();
			return dataType;
		}
		catch(Exception e)
		{
			System.out.println("Error in getting the data type");
			System.out.println(e);
		}
		return dataType;
	}

	public static String[] Get_Column_Name(String table)
	{
		String[] column = new String[0];
		try
		{
			RandomAccessFile file = new RandomAccessFile(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME + "/" + "davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
				table = QueryHandler.ActiveDBName+"."+table;
			String[] comparator = {"table_name","=",table};
			filter(file, comparator, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[2]);
			}
			column = array.toArray(new String[array.size()]);
			file.close();
			return column;
		}
		catch(Exception e)
		{
			System.out.println("Error in getting the column name");
			System.out.println(e);
		}
		return column;
	}

	public static String[] Get_Nullable(String tableName)
	{
		String[] n = new String[0];
		try
		{
			RandomAccessFile file = new RandomAccessFile(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME + "/" + "davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!tableName.equals("davisbase_columns") && !tableName.equals("davisbase_tables"))
				tableName = QueryHandler.ActiveDBName+"."+tableName;
			String[] comparator = {"table_name","=",tableName};
			filter(file, comparator, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values())
			{
				array.add(i[5]);
			}
			n = array.toArray(new String[array.size()]);
			file.close();
			return n;
		}
		catch(Exception e)
		{
			System.out.println("Unable to get nullable property");
			System.out.println(e);
		}
		return n;
	}

	public static void selectQuery(String file, String tableName, String[] columnList, String[] comparators)
	{
		try
		{
			Buffer buffer = new Buffer();
			String[] columnName = Get_Column_Name(tableName);
			String[] dataType = Get_Data_Type(tableName);

			RandomAccessFile rFile = new RandomAccessFile(file, "rw");
			filter(rFile, comparators, columnName, dataType, buffer);
			buffer.display(columnList);
			rFile.close();
		}
		catch(Exception e)
		{
			System.out.println("Unable to select");
			System.out.println(e);
		}
	}

	public static void delete(String tableName, String[] comparators)
	{
			
		try {
			int key = -1;
			RandomAccessFile file = new RandomAccessFile(Constants.DEFAULT_DIRNAME + "/" +QueryHandler.ActiveDBName + "/" + tableName + "/" + tableName + ".tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = Get_Column_Name(tableName);
			String[] type = Get_Data_Type(tableName);
			filter(file, comparators, columnName, type, buffer);
			boolean flag=false;
			for(String[] i : buffer.content.values()){
				if(flag)
					break;
				for(int j = 0; j < i.length; j++)
					if(buffer.columnName[j].equals(comparators[0]) && i[j].equals(comparators[2]))
					{
						key =(Integer.parseInt(i[0]));							
						flag = true;
						break;
					}
			
			}
				
			int numOfPages = pages(file);
			int page = 1;

			for(int p = 1; p <= numOfPages; p++)
				if(UtilsPage.Has_Key(file, p, key))
				{
					page = p;
				}
			int[] array = UtilsPage.Get_Key_Array(file, page);
			int id = 0;
			for(int i = 0; i < array.length; i++)
				if(array[i] == key)
					id = i;
			int offset = UtilsPage.Get_Offset_Cell(file, page, id);
			long location = UtilsPage.get_Cell_Location(file, page, id);
			String[] array_s = Get_Column_Name(tableName);
		
			String[] values = Retrieve_Payload(file, location);

			byte[] shortCodes = new byte[array_s.length-1];
			int payLoadSize = Calculate_Payload(tableName, values, shortCodes);
			file.seek((page-1)*pageSize+offset);
			file.writeShort(payLoadSize);
			file.writeInt(-10000);
			
			file.close();

		} catch (Exception e) 
		{
			e.printStackTrace();
		}
		
	}
	public static void filter(RandomAccessFile file, String[] comparators, String[] columnName, String[] type, Buffer buffer)
	{
		try{
			int numOfPages = pages(file);
			for(int page = 1; page <= numOfPages; page++)
			{
				file.seek((page-1)*Constants.PAGE_SIZE);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else
				{
					byte numOfCells = UtilsPage.Get_Cell_Num(file, page);

					for(int i=0; i < numOfCells; i++)
					{
						long location = UtilsPage.get_Cell_Location(file, page, i);
						file.seek(location+2);
						int rowid = file.readInt();

						String[] payload = Retrieve_Payload(file, location);

						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = "'"+payload[j]+"'";

						boolean present = Comparator_Check(payload, rowid, comparators, columnName);

						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = payload[j].substring(1, payload[j].length()-1);

						if(present)
							buffer.add(rowid, payload);
					}
				}
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}
		catch(Exception e)
		{
			System.out.println("Unable to filter data");
			e.printStackTrace();
		}

	}

	public static void filter(RandomAccessFile file, String[] comparator, String[] columnName, Buffer buffer){
		try
		{
			int numOfPages = pages(file);
			for(int page = 1; page <= numOfPages; page++)
			{
				file.seek((page-1)*Constants.PAGE_SIZE);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else
				{
					byte numCells = UtilsPage.Get_Cell_Num(file, page);

					for(int i=0; i < numCells; i++)
					{
						long location = UtilsPage.get_Cell_Location(file, page, i);
						file.seek(location+2);
						int rowid = file.readInt();
						String[] payload = Retrieve_Payload(file, location);

						boolean present = Comparator_Check(payload, rowid, comparator, columnName);
						if(present)
							buffer.add(rowid, payload);
					}
				}
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}catch(Exception e)
		{
			System.out.println("Unable to filter data");
			e.printStackTrace();
		}

	}

	public static int pages(RandomAccessFile file)
	{
		int numOfPages = 0;
		try
		{
			numOfPages = (int)(file.length()/(new Long(pageSize)));
		}
		catch(Exception e)
		{
			System.out.println("Unable to fetch pages");
		}

		return numOfPages;
	}

	public static boolean Comparator_Check(String[] payload, int rowid, String[] comparators, String[] columnName)
	{

		boolean present = false;
		if(comparators.length == 0)
		{
			present = true;
		}
		else
		{
			int colPos = 1;
			for(int i = 0; i < columnName.length; i++){
				if(columnName[i].equals(comparators[0]))
				{
					colPos = i + 1;
					break;
				}
			}
			String operator = comparators[1];
			String value = comparators[2];
			if(colPos == 1)
			{
				switch(operator)
				{
					case "=": if(rowid == Integer.parseInt(value)) 
								present = true;
							  else
							  	present = false;
							  break;
					case "<": if(rowid < Integer.parseInt(value)) 
								present = true;
							  else
								present = false;
							  break;
					case ">": if(rowid > Integer.parseInt(value)) 
								present = true;
							  else
							  	present = false;
							break;
					case "<=": if(rowid <= Integer.parseInt(value)) 
								present = true;
							  else
							  	present = false;	
							  break;
					case ">=": if(rowid >= Integer.parseInt(value)) 
								present = true;
							  else
							  	present = false;	
							  break;
					
					case "<>": if(rowid != Integer.parseInt(value))  
								present = true;
							  else
							  	present = false;	
							  break;						  							  							  							
				}
			}
			else
			{
				if(value.equals(payload[colPos-1]))
					present = true;
				else
					present = false;
			}
		}
		return present;
	}

	public static void initializeDataStore() 
	{
		try {
			File catalogFile = new File(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME);
			String[] oldFiles;
			oldFiles = catalogFile.list();
			for (int i=0; i<oldFiles.length; i++) 
			{
				File File = new File(catalogFile, oldFiles[i]); 
				File.delete();
			}
		}
		catch (SecurityException e) 
		{
			System.out.println("Unable to initialize meta data :"+ e);
			
		}

		try {
			davisbaseTablesCatalog = new RandomAccessFile(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME + "/" + "davisbase_tables.tbl", "rw");
			davisbaseTablesCatalog.setLength(pageSize);
			davisbaseTablesCatalog.seek(0);
			davisbaseTablesCatalog.write(0x0D);
			davisbaseTablesCatalog.write(0x02);
			int[] offset=new int[2];
			int size1=24;
			int size2=25;
			offset[0]=pageSize-size1;
			offset[1]=offset[0]-size2;
			davisbaseTablesCatalog.writeShort(offset[1]);
			davisbaseTablesCatalog.writeInt(0);
			davisbaseTablesCatalog.writeInt(10);
			davisbaseTablesCatalog.writeShort(offset[1]);
			davisbaseTablesCatalog.writeShort(offset[0]);
			davisbaseTablesCatalog.seek(offset[0]);
			davisbaseTablesCatalog.writeShort(20);
			davisbaseTablesCatalog.writeInt(1); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(28);
			davisbaseTablesCatalog.writeBytes("davisbase_tables");
			davisbaseTablesCatalog.seek(offset[1]);
			davisbaseTablesCatalog.writeShort(21);
			davisbaseTablesCatalog.writeInt(2); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(29);
			davisbaseTablesCatalog.writeBytes("davisbase_columns");
		}
		catch (Exception e) 
		{
			System.out.println("Unable to create meta data");
			System.out.println(e);
		}
		
		try 
		{
			davisbaseColumnsCatalog = new RandomAccessFile(Constants.DEFAULT_DIRNAME + "/" + Constants.DEFAULT_DATABASENAME + "/" + "davisbase_columns.tbl", "rw");
			davisbaseColumnsCatalog.setLength(pageSize);
			davisbaseColumnsCatalog.seek(0);       
			davisbaseColumnsCatalog.writeByte(0x0D);
			davisbaseColumnsCatalog.writeByte(0x08);
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
			davisbaseColumnsCatalog.writeShort(offset[8]);
			davisbaseColumnsCatalog.writeInt(0);
			davisbaseColumnsCatalog.writeInt(0);
			for(int i=0;i<9;i++)
				davisbaseColumnsCatalog.writeShort(offset[i]);
			davisbaseColumnsCatalog.seek(offset[0]);
			davisbaseColumnsCatalog.writeShort(33);
			davisbaseColumnsCatalog.writeInt(1); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");
			davisbaseColumnsCatalog.writeByte(1);
			davisbaseColumnsCatalog.writeBytes("NO");			
			davisbaseColumnsCatalog.seek(offset[1]);
			davisbaseColumnsCatalog.writeShort(39);
			davisbaseColumnsCatalog.writeInt(2); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("table_name");  
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2);
			davisbaseColumnsCatalog.writeBytes("NO");			
			davisbaseColumnsCatalog.seek(offset[2]);
			davisbaseColumnsCatalog.writeShort(34);
			davisbaseColumnsCatalog.writeInt(3); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");
			davisbaseColumnsCatalog.writeByte(1);
			davisbaseColumnsCatalog.writeBytes("NO");			
			davisbaseColumnsCatalog.seek(offset[3]);
			davisbaseColumnsCatalog.writeShort(40);
			davisbaseColumnsCatalog.writeInt(4); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("table_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2);
			davisbaseColumnsCatalog.writeBytes("NO");
			davisbaseColumnsCatalog.seek(offset[4]);
			davisbaseColumnsCatalog.writeShort(41);
			davisbaseColumnsCatalog.writeInt(5); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("column_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(3);
			davisbaseColumnsCatalog.writeBytes("NO");			
			davisbaseColumnsCatalog.seek(offset[5]);
			davisbaseColumnsCatalog.writeShort(39);
			davisbaseColumnsCatalog.writeInt(6); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(21);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("data_type");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeBytes("NO");
			davisbaseColumnsCatalog.seek(offset[6]);
			davisbaseColumnsCatalog.writeShort(49);
			davisbaseColumnsCatalog.writeInt(7); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(19);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("ordinal_position");
			davisbaseColumnsCatalog.writeBytes("TINYINT");
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeBytes("NO");
			davisbaseColumnsCatalog.seek(offset[7]);
			davisbaseColumnsCatalog.writeShort(41);
			davisbaseColumnsCatalog.writeInt(8); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("is_nullable");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(6);
			davisbaseColumnsCatalog.writeBytes("NO");
		}
		catch (Exception e) 
		{
			System.out.println("Unable to create the database_columns file");
			System.out.println(e);
		}
	}
}