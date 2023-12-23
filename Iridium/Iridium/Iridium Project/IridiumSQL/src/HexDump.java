import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.lang.Math.*;
import java.io.IOException;
import static java.lang.System.out;
public class HexDump {
	static String columnGap = " ";
	static int pageSize = 0x200;
	static RandomAccessFile raf;
	static String displayControlCharacterAs = ".";
	static int currentByteColumn = 0;
	static boolean displayASCII = true;
	static boolean displayPageHeader = true;
	static boolean displayHelp = false;

	public static void main(String[] args) {

		displayCopyright();
		if(args.length == 0) {
			out.println();
			out.println("ERROR: Must supply a file name to be displayed");
			out.println("USAGE: java HexDump <file_name>");
			out.println();
			System.exit(0);
		}

		try {
			raf = new RandomAccessFile(args[0],"r");
			displayHexDump();
		}
		catch (IOException e) {
			out.println(e);
		}
	}
	static void displayHexDump() {
		/* This try block is needed because RandomAccessFile is used */
		try {
			raf.seek(0); 
			int thisByteOffset = 0;
			long size = raf.length(); 
			byte[] rowOfBytes = new byte[16];
			while(thisByteOffset < size) {
				if(thisByteOffset % pageSize == 0) {
					printPageHeader();
				}
				if(thisByteOffset % 16 == 0) {
					out.print(String.format("%08x  ", thisByteOffset));
					currentByteColumn = 0;
				}
				{
					int ndx = thisByteOffset % 16;
					rowOfBytes[ndx] = raf.readByte();
					thisByteOffset++;
					currentByteColumn++;
				}
				if(thisByteOffset % 16 == 0) {
					printRowOfBytes(rowOfBytes);
					rowOfBytes = new byte[16];
				}
				currentByteColumn++;
			}
			printRowOfBytes(rowOfBytes);
			out.println("currentByteColumn: " + currentByteColumn);
			out.println();
		}
		catch (IOException e) {
			out.println(e);
		}
	}

	static void printPageHeader() {

		out.println();
		out.print("Address    0  1  2  3  4  5  6  7 " + columnGap + " 8  9  A  B  C  D  E  F");
		if(displayASCII)
			out.print("  |0123456789ABCDEF|");
		out.println();


		out.print(line(58,"-"));
		if(displayASCII)
			out.print(line(20,"-"));
		out.println();


	}
	static void printRowOfBytes(byte[] row) {
		int rowLength = row.length;
		for(int n = 0; n < rowLength; n++) {
			if(n==8)
				out.print(columnGap);
			out.print(String.format("%02X ", row[n]));
		}
		
		if(displayASCII) {
			out.print(" |");
			for(int n = 0; n < rowLength; n++) {
				if(row[n] < 0x20 || row[n] > 0x7e)
					out.print(displayControlCharacterAs);
				else
					out.print((char)row[n]);
			}
			out.print("|");
		}
		
		out.println();
	}

	static void displayCopyright() {
		out.println("*");
		out.println("* HexDump (c)2018 Chris Irwin Davis");
		out.println("*");
	}

	static String line(int length, String c) {
		String s = "";
		while(length>0) {
			s = s + c;
			length--;
		}
		return s;
	}
}