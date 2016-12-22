package homework1;

import java.util.ArrayList;
import java.util.HashMap;
import homework1.Station;

public class Sequential {

	//Data structure that contains the 1-1 memory copy of the input data
	public static ArrayList<String> inputData = new ArrayList<String>();

	//Hash map with key as stationID and value with the station object
	public static HashMap<String, Station> stations = new HashMap<String, Station>();

	public static void performSequentialAccumulation(){
		for(String line: inputData){
			String[] str = line.split(",");
			if(str[2] == null || !str[2].trim().equalsIgnoreCase("TMAX")){
				continue;
			} else {
				String key = str[0];
				if(stations.containsKey(key)){
					Station stn = stations.get(key);
					int tempr = Integer.parseInt(str[3]);
					stn.addTemperatureSum(tempr);
					stn.incrementCount();
					stations.put(key, stn);
				} else {
					int tempr = Integer.parseInt(str[3]);
					Station stn = new Station(key, tempr);
					stations.put(key, stn);
				}
			}
		}
	}

	public static HashMap<String, Station> performSequential(ArrayList<String> input) {		
		inputData= input;
		performSequentialAccumulation();
		return stations;
	}

}
