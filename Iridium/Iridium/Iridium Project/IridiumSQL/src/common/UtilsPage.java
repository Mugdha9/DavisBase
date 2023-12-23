package common;
 
import java.text.SimpleDateFormat;
import CommandsQuery.QueryHandler;
import java.io.RandomAccessFile;
import java.util.Date;
 
public class UtilsPage {
    public static int pageSize = 512;
    public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
 

    public static int Make_Interior_Page(RandomAccessFile f) 
	{
        int numOfPages = 0;
        try 
		{
            numOfPages = (int) (f.length() / (new Long(pageSize)));
            numOfPages = numOfPages + 1;
            f.setLength(Constants.PAGE_SIZE * numOfPages);
            f.seek((numOfPages - 1) * Constants.PAGE_SIZE);
            f.writeByte(0x05);
			for(int l=0;l<numOfPages;l++)
			{
				l++;
				int key = l;
			}
        } 
		catch (Exception e) 
		{
            System.out.println("Unable to make interior page");
        }
 
        return numOfPages;
    }
 
    public static int Make_Leaf_Page(RandomAccessFile f) 
	{
        int numOfPages = 0;
        try 
		{
            numOfPages = (int) (f.length() / (new Long(pageSize)));
            numOfPages = numOfPages + 1;
            f.setLength(Constants.PAGE_SIZE * numOfPages);
            f.seek((numOfPages - 1) * Constants.PAGE_SIZE);
            f.writeByte(0x0D);
			for(int l=0;l<numOfPages;l++)
			{
				l++;
				int key = l;
			}
        } 
		catch (Exception e) 
		{
            System.out.println("Unable to make leafpage");
        }
 
        return numOfPages;
 
    }
 
    public static int Find_Middle_Key(RandomAccessFile f, int p) {
        int value = 0;
        try 
		{
            f.seek((p - 1) * pageSize);
            byte pageCategory = f.readByte();
            int numOfCells = Get_Cell_Num(f, p);
            int middle = (int) Math.ceil((double) numOfCells / 2);
            long location = get_Cell_Location(f, p, middle - 1);
            f.seek(location);
 
            switch (pageCategory) 
			{
				case 0x0D:
                    value = f.readShort();
                    value = f.readInt();
                    break;
                case 0x05:
                    value = f.readInt();
                    value = f.readInt();
                    break;
                
            }
 
        } catch (Exception e) 
		{
            System.out.println("Unable to find middle key");
        }
 
        return value;
    }
 
    public static void Split_LeafPage(RandomAccessFile f, int cPage, int nPage) 
	{
        try {
            int numOfCells = Get_Cell_Num(f, cPage);
            int middle = (int) Math.ceil((double) numOfCells / 2);
 
            int numCellA = middle - 1;
            int numCellB = numOfCells - numCellA;
            int content = 512;
 
            for (int i = numCellA; i < numOfCells; i++) {
                long location = get_Cell_Location(f, cPage, i);
                f.seek(location);
                int cellSize = f.readShort() + 6;
                content = content - cellSize;
                f.seek(location);
                byte[] cell = new byte[cellSize];
                f.read(cell);
                f.seek((nPage - 1) * Constants.PAGE_SIZE + content);
                f.write(cell);
                Set_Offset_Cell(f, nPage, i - numCellA, content);
            }
            f.seek((nPage - 1) * Constants.PAGE_SIZE + 2);
            f.writeShort(content);
            short offset = Get_Offset_Cell(f, cPage, numCellA - 1);
            f.seek((cPage - 1) * Constants.PAGE_SIZE + 2);
            f.writeShort(offset);
            int rightMost = get_RightMost_Page(f, cPage);
            Set_Right_Most(f, nPage, rightMost);
            Set_Right_Most(f, cPage, nPage);
            int parent = Get_Parent(f, cPage);
            Set_Parent(f, nPage, parent);
            byte number = (byte) numCellA;
            Set_Cell_Num(f, cPage, number);
            number = (byte) numCellB;
            Set_Cell_Num(f, nPage, number);
        } 
		catch (Exception e) 
		{
            System.out.println("Unable to split leaf page");
            e.printStackTrace();
        }
    }
	
	public static short Calculate_Payload(String[] values, String[] dataTypes) 
	{
        int value = 1 + dataTypes.length - 1;
        for (int i = 1; i < dataTypes.length; i++) {
            String currentDataType = dataTypes[i];
            switch (currentDataType) 
			{
                case "TINYINT":
                    value = value + 1;
                    break;
                case "SMALLINT":
                    value = value + 2;
                    break;
                case "INT":
                    value = value + 4;
                    break;
                case "BIGINT":
                    value = value + 8;
                    break;
                case "REAL":
                    value = value + 4;
                    break;
                case "DOUBLE":
                    value = value + 8;
                    break;
                case "DATETIME":
                    value = value + 8;
                    break;
                case "DATE":
                    value = value + 8;
                    break;
                case "TEXT":
                    String text = values[i];
                    int len = text.length();
                    value = value + len;
                    break;
                default:
                    break;
            }
        }
        return (short) value;
    }

    public static void Split_Interior_Pages(RandomAccessFile f, int cPage, int nPage) {
        try 
		{
            int numOfCells = Get_Cell_Num(f, cPage);
            int middle = (int) Math.ceil((double) numOfCells / 2);
 
            int numCellA = middle - 1;
            int numCellB = numOfCells - numCellA - 1;
            short content = 512;
 
            for (int i = numCellA + 1; i < numOfCells; i++) 
			{
                long location = get_Cell_Location(f, cPage, i);
                short cellSize = 8;
                content = (short) (content - cellSize);
                f.seek(location);
                byte[] cell = new byte[cellSize];
                f.read(cell);
                f.seek((nPage - 1) * pageSize + content);
                f.write(cell);
                f.seek(location);
                int page = f.readInt();
                Set_Parent(f, page, nPage);
                Set_Offset_Cell(f, nPage, i - (numCellA + 1), content);
            }
            int temp = get_RightMost_Page(f, cPage);
            Set_Right_Most(f, nPage, temp);
            long midLoc = get_Cell_Location(f, cPage, middle - 1);
            f.seek(midLoc);
            temp = f.readInt();
            Set_Right_Most(f, cPage, temp);
            f.seek((nPage - 1) * pageSize + 2);
            f.writeShort(content);
            short offset = Get_Offset_Cell(f, cPage, numCellA - 1);
            f.seek((cPage - 1) * pageSize + 2);
            f.writeShort(offset);
            int parent = Get_Parent(f, cPage);
            Set_Parent(f, nPage, parent);
            byte num = (byte) numCellA;
            Set_Cell_Num(f, cPage, num);
            num = (byte) numCellB;
            Set_Cell_Num(f, nPage, num);
        } 
		catch (Exception e) 
		{
            System.out.println("Unable to split interior page");
        }
    }

	public static void Sort_Cell_Array(RandomAccessFile file, int page) {
        byte number = Get_Cell_Num(file, page);
        int[] keys = Get_Key_Array(file, page);
        short[] cellArray = Get_Array_Cell(file, page);
        int left;
        short right;
 
        for (int i = 1; i < number; i++) 
		{
            for (int j = i; j > 0; j--)
			{
                if (keys[j] < keys[j - 1]) 
				{
 
                    left = keys[j];
                    keys[j] = keys[j - 1];
                    keys[j - 1] = left;
 
                    right = cellArray[j];
                    cellArray[j] = cellArray[j - 1];
                    cellArray[j - 1] = right;
                }
            }
        }
 
        try 
		{
            file.seek((page - 1) * Constants.PAGE_SIZE + 12);
            for (int i = 0; i < number; i++) 
			{
                file.writeShort(cellArray[i]);
            }
        } catch (Exception e) 
		{
            System.out.println("Unable to sort cell array");
        }
    }
 
    public static void Split_Leaf(RandomAccessFile f, int p) {
        int newPage = Make_Leaf_Page(f);
        int middle = Find_Middle_Key(f, p);
        Split_LeafPage(f, p, newPage);
        int par = Get_Parent(f, p);
        if (par == 0) 
		{
            int rootPage = Make_Interior_Page(f);
            Set_Parent(f, p, rootPage);
            Set_Parent(f, newPage, rootPage);
            Set_Right_Most(f, rootPage, newPage);
            Insert_Interior_Cell(f, rootPage, p, middle);
        } 
		else 
		{
            long parLocation = Get_Pointer_Location(f, p, par);
            Set_Pointer_Location(f, parLocation, par, newPage);
            Insert_Interior_Cell(f, par, p, middle);
            Sort_Cell_Array(f, par);
            while (Interior_Space_Present(f, par)) 
			{
                par = Split_Interior(f, par);
            }
        }
    }
 
	//splits the pages when maximum limit reached
    public static int Split_Interior(RandomAccessFile file, int page) {
        int newPage = Make_Interior_Page(file);
        int middle = Find_Middle_Key(file, page);
        Split_Interior_Pages(file, page, newPage);
        int parent = Get_Parent(file, page);
        if (parent == 0)
		{
            int rootPage = Make_Interior_Page(file);
            Set_Parent(file, page, rootPage);
            Set_Parent(file, newPage, rootPage);
            Set_Right_Most(file, rootPage, newPage);
            Insert_Interior_Cell(file, rootPage, page, middle);
            return rootPage;
        } 
		else 
		{
            long ploc = Get_Pointer_Location(file, page, parent);
            Set_Pointer_Location(file, ploc, parent, newPage);
            Insert_Interior_Cell(file, parent, page, middle);
            Sort_Cell_Array(file, parent);
            return parent;
        }
    }
 
	//finds key element of every array
    public static int[] Get_Key_Array(RandomAccessFile f, int p) {
        int number = new Integer(Get_Cell_Num(f, p));
        int[] array = new int[number];
 
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE);
            byte pageCategory = f.readByte();
            byte offset = 0;
            switch (pageCategory) 
			{
				case 0x0d:
                    offset = 2;
                    break;
                case 0x05:
                    offset = 4;
                    break;
                default:
                    offset = 2;
                    break;
            }
 
            for (int i = 0; i < number; i++) {
                long loc = get_Cell_Location(f, p, i);
                f.seek(loc + offset);
                array[i] = f.readInt();
            }
 
        } catch (Exception e) {
            System.out.println("Unable to get keys");
        }
 
        return array;
    }
 
	//Finds Record's Address
    public static short[] Get_Array_Cell(RandomAccessFile f, int p) {
        int number = new Integer(Get_Cell_Num(f, p));
        short[] array = new short[number];
 
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 12);
            for (int i = 0; i < number; i++) {
                array[i] = f.readShort();
            }
			for(int key=1;key<number;key++)
			{
				int offset = key + 1;
			}
        } catch (Exception e) {
            System.out.println("Unable to get cell array");
        }
 
        return array;
    }
 
	//Sets parent of page
    public static void Set_Parent(RandomAccessFile f, int p, int par) {
        try 
		{
            f.seek((p - 1) * Constants.PAGE_SIZE + 8);
            f.writeInt(par);
        } catch (Exception e) 
		{
            System.out.println("Unable to set parent");
        }
    }

	//Fetches the parent page after splitting
	public static int Get_Parent(RandomAccessFile f, int p) {
        int value = 0;
 
        try 
		{
            f.seek((p - 1) * Constants.PAGE_SIZE + 8);
            value = f.readInt();
        } 
		catch (Exception e)
		{
            System.out.println("Unable to get parent");
        }
 
        return value;
    }
 
	//Finds the location of the pointer in the page
    public static long Get_Pointer_Location(RandomAccessFile f, int p, int parent) {
        long value = 0;
        try {
            int numOfCells = new Integer(Get_Cell_Num(f, parent));
            for (int i = 0; i < numOfCells; i++) 
			{
                long location = get_Cell_Location(f, parent, i);
                f.seek(location);
                int childPage = f.readInt();
                if (childPage == p) {
                    value = location;
                }
            }
        } catch (Exception e) {
            System.out.println("Unable to get pointer location");
        }
 
        return value;
    }
 
	//Sets pointer to appropriate location in page
    public static void Set_Pointer_Location(RandomAccessFile file, long location, int par, int page) {
        try 
		{
            if (location == 0) 
			{
                file.seek((par - 1) * pageSize + 4);
            } else 
			{
                file.seek(location);
            }
            file.writeInt(page);
        } catch (Exception e) 
		{
            System.out.println("Unable to set pointer location");
        }
    }
 
	//Insert user data in interior cell after splitting
    public static void Insert_Interior_Cell(RandomAccessFile f, int p, int child, int key) {
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 2);
            short cont = f.readShort();
            if (cont == 0)
                cont = 512;
            cont = (short) (cont - 8);
            f.seek((p - 1) * Constants.PAGE_SIZE + cont);
            f.writeInt(child);
            f.writeInt(key);
            f.seek((p - 1) * Constants.PAGE_SIZE + 2);
            f.writeShort(cont);
            byte num = Get_Cell_Num(f, p);
            Set_Offset_Cell(f, p, num, cont);
            num = (byte) (num + 1);
            Set_Cell_Num(f, p, num);
 
        } catch (Exception e) {
            System.out.println("Unable to insert Interior cell");
        }
    }
	
	//insert data in leaf cell
    public static void Insert_Leaf_Cell(RandomAccessFile f, int p, int os, short payloadSize, int key, byte[] dataTypeCode,
            String[] vals, String table) {
        try {
            String s;
            f.seek((p - 1) * pageSize + os);
            String[] colName = UtilsTable.Get_Column_Name(table);
            if (!table.equals("davisbase_columns") && !table.equals("davisbase_tables")) {
 
                RandomAccessFile IndexFile = new RandomAccessFile(
                        Constants.DEFAULT_DIRNAME + "/" + QueryHandler.ActiveDBName + "/" + table + "/" + colName[0] + ".ndx", "rw");
                IndexFile.seek(IndexFile.length());
                IndexFile.writeInt(key);
                IndexFile.writeLong(f.getFilePointer());
                IndexFile.close();
 
                for (int i = 1; i < vals.length; i++) {
                    IndexFile = new RandomAccessFile(
                            Constants.DEFAULT_DIRNAME + "/" + QueryHandler.ActiveDBName + "/" + table + "/" + colName[i] + ".ndx", "rw");
                    IndexFile.seek(IndexFile.length());
                    switch (dataTypeCode[i - 1]) {
						case 0x0A:
                            s = vals[i];
                            Date temp = new SimpleDateFormat(datePattern).parse(s);
                            long time = temp.getTime();
                            IndexFile.writeLong(time);
                            break;
                        case 0x0B:
                            s = vals[i];
                            s = s + "_00:00:00";
                            Date temp2 = new SimpleDateFormat(datePattern).parse(s);
                            long time2 = temp2.getTime();
                            IndexFile.writeLong(time2);
                            break;
                        case 0x00:
                            IndexFile.writeByte(0);
                            break;
                        case 0x01:
                            IndexFile.writeShort(0);
                            break;
                        case 0x02:
                            IndexFile.writeInt(0);
                            break;
                        case 0x03:
                            IndexFile.writeLong(0);
                            break;
                        case 0x04:
                            IndexFile.writeByte(new Byte(vals[i]));
                            break;
                        case 0x05:
                            IndexFile.writeShort(new Short(vals[i]));
                            break;
                        case 0x06:
                            IndexFile.writeInt(new Integer(vals[i]));
                            break;
                        case 0x07:
                            IndexFile.writeLong(new Long(vals[i]));
                            break;
                        case 0x08:
                            IndexFile.writeFloat(new Float(vals[i]));
                            break;
                        case 0x09:
                            IndexFile.writeDouble(new Double(vals[i]));
                            break;
                        default:
                            f.writeBytes(vals[i]);
                            break;
 
                    }
 
                    IndexFile.writeLong(f.getFilePointer());
                    IndexFile.close();
                }
 
            }
 
            f.seek((p - 1) * Constants.PAGE_SIZE + os);
            f.writeShort(payloadSize);
            f.writeInt(key);
            int col = vals.length - 1;
 
            f.writeByte(col);
            f.write(dataTypeCode);
 
            for (int i = 1; i < vals.length; i++)
 
            {
                switch (dataTypeCode[i - 1]) {
					case 0x0A:
                        s = vals[i];
                        Date temp = new SimpleDateFormat(datePattern).parse(s);
                        long time = temp.getTime();
                        f.writeLong(time);
                        break;
                    case 0x0B:
                        s = vals[i];
                        s = s + "_00:00:00";
                        Date temp2 = new SimpleDateFormat(datePattern).parse(s);
                        long time2 = temp2.getTime();
                        f.writeLong(time2);
                        break;
                    case 0x00:
                        f.writeByte(0);
                        break;
                    case 0x01:
                        f.writeShort(0);
                        break;
                    case 0x02:
                        f.writeInt(0);
                        break;
                    case 0x03:
                        f.writeLong(0);
                        break;
                    case 0x04:
                        f.writeByte(new Byte(vals[i]));
                        break;
                    case 0x05:
                        f.writeShort(new Short(vals[i]));
                        break;
                    case 0x06:
                        f.writeInt(new Integer(vals[i]));
                        break;
                    case 0x07:
                        f.writeLong(new Long(vals[i]));
                        break;
                    case 0x08:
                        f.writeFloat(new Float(vals[i]));
                        break;
                    case 0x09:
                        f.writeDouble(new Double(vals[i]));
                        break;
                    default:
                        f.writeBytes(vals[i]);
                        break;
                }
            }
            int numOfCells = Get_Cell_Num(f, p);
            byte temp = (byte) (numOfCells + 1);
            Set_Cell_Num(f, p, temp);
            f.seek((p - 1) * Constants.PAGE_SIZE + 12 + numOfCells * 2);
            f.writeShort(os);
            f.seek((p - 1) * Constants.PAGE_SIZE + 2);
            int content = f.readShort();
            if (content >= os || content == 0) {
                f.seek((p - 1) * Constants.PAGE_SIZE + 2);
                f.writeShort(os);
            }
        } catch (Exception e) {
            System.out.println("Unable to insert in leaf cell");
            e.printStackTrace();
        }
    }

	//checks if keys is present in the page
    public static boolean Has_Key(RandomAccessFile f, int p, int k) {
        int[] array = Get_Key_Array(f, p);
        for (int i : array)
            if (k == i)
                return true;
        return false;
    }
 
	//gets cell id location
    public static long get_Cell_Location(RandomAccessFile f, int p, int id) {
        long location = 0;
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 12 + id * 2);
            short offset = f.readShort();
            long orig = (p - 1) * pageSize;
            location = orig + offset;
			for(int j=0;j<p;j++)
			{
				j = j + 1;
			}
        } catch (Exception e) {
            System.out.println("Unable to find cell location");
        }
        return location;
    }

 
	//Updates the left sibling or child of current page
    public static void Update_Leaf_Cell(RandomAccessFile f, int p, int os, int payLoadSize, int key, byte[] dataTypeCodes,
            String[] vals, String table) {
        try {
            String s;
            f.seek((p - 1) * pageSize + os);
            f.writeShort(payLoadSize);
            f.writeInt(key);
            int col = vals.length - 1;
            f.writeByte(col);
            f.write(dataTypeCodes);
            for (int i = 1; i < vals.length; i++) {
                switch (dataTypeCodes[i - 1]) {
					case 0x0A:
                        s = vals[i];
                        Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length() - 1));
                        long time = temp.getTime();
                        f.writeLong(time);
                        break;
                    case 0x0B:
                        s = vals[i];
                        s = s.substring(1, s.length() - 1);
                        s = s + "_00:00:00";
                        Date temp2 = new SimpleDateFormat(datePattern).parse(s);
                        long time2 = temp2.getTime();
                        f.writeLong(time2);
                        break;
                    case 0x00:
                        f.writeByte(0);
                        break;
                    case 0x01:
                        f.writeShort(0);
                        break;
                    case 0x02:
                        f.writeInt(0);
                        break;
                    case 0x03:
                        f.writeLong(0);
                        break;
                    case 0x04:
                        f.writeByte(new Byte(vals[i]));
                        break;
                    case 0x05:
                        f.writeShort(new Short(vals[i]));
                        break;
                    case 0x06:
                        f.writeInt(new Integer(vals[i]));
                        break;
                    case 0x07:
                        f.writeLong(new Long(vals[i]));
                        break;
                    case 0x08:
                        f.writeFloat(new Float(vals[i]));
                        break;
                    case 0x09:
                        f.writeDouble(new Double(vals[i]));
                        break;
                    default:
                        f.writeBytes(vals[i]);
                        break;
                }
            }
        } catch (Exception e) {
            System.out.println("Unable to update leaf cell");
            System.out.println(e);
        }
    }

 
	//Get's the right sibling or child
    public static int get_RightMost_Page(RandomAccessFile f, int p) {
        int value = 0;
 
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 4);
            value = f.readInt();
        } catch (Exception e) {
            System.out.println("Unable to get right most page");
        }
 
        return value;
    }

	//Set's the right sibling or child to rm
    public static void Set_Right_Most(RandomAccessFile f, int p, int rm) {
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 4);
            f.writeInt(rm);
        } catch (Exception e) {
            System.out.println("Unabel to set page");
        }
 
    }
 
    // Return the number of cells in the page
    public static byte Get_Cell_Num(RandomAccessFile f, int p) {
        byte value = 0;
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 1);
            value = f.readByte();
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("Unable to get cell's number");
        }
        return value;
    }
 
	//Set's total value of cells in page to number
    public static void Set_Cell_Num(RandomAccessFile f, int p, byte number) {
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 1);
            f.writeByte(number);
        } catch (Exception e) {
            System.out.println("Unable to set cell's number");
        }
    }
 
	//checks if there is space present in interior page
    public static boolean Interior_Space_Present(RandomAccessFile f, int p) {
        byte numOfCells = Get_Cell_Num(f, p);
        if (numOfCells > 30)
            return true;
        else
            return false;
    }
 
	
	
	//Sets offset of id cell
    public static void Set_Offset_Cell(RandomAccessFile f, int p, int id, int os) {
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 12 + id * 2);
            f.writeShort(os);
			for(int j=0;j<p;j++)
			{
				j = j + 1;
			}
        } catch (Exception e) {
            System.out.println("Unable to set offset");
        }
    }

	//Finds offset of id cell
    public static short Get_Offset_Cell(RandomAccessFile f, int p, int id) {
        short os = 0;
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 12 + id * 2);
            os = f.readShort();
			int key = os; //checking key
			for(int j=0;j<p;j++)
			{
				j = j + 1;
			}
        } catch (Exception e) {
            System.out.println("Unable to get cell offset");
        }
        return os;
    }

	//checks if space is available to insert new row in page
    public static int Leaf_Space_Present(RandomAccessFile f, int p, int s) {
        int value = -1;
		int key;
        try {
            f.seek((p - 1) * Constants.PAGE_SIZE + 2);
            int cont = f.readShort();
            if (cont == 0)
                return Constants.PAGE_SIZE - s;
            int numOfCells = Get_Cell_Num(f, p);
            int sp = cont - 20 - 2 * numOfCells;
            if (s < sp)
                return cont - s;
			if(sp > 0)
			{
				for(int j=0;j<sp;j++)
				{
					key = j + sp;
				}
			}
 
        } catch (Exception e) {
            System.out.println("Unable to find space");
        }
 
        return value;
    }
}