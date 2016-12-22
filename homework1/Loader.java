package homework1;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class Loader {
	
	public static Scanner sc;
	public static ArrayList<String> loadInputFile(String inputFileName){
		ArrayList<String> str = new ArrayList<String>();
		File inputFile = new File(inputFileName);
		try {
			sc = new Scanner(inputFile);
			sc.useDelimiter("\n");
			while(sc.hasNext()){
				String s = sc.next();
				str.add(s);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return str;
	}
	
}
