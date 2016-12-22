package homework1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NoSharing {

	//Data structure that contains the 1-1 memory copy of the input data
	public static ArrayList<String> inputData = new ArrayList<String>();

	//Hash map with key as stationID and value with the station object
	public static HashMap<String, Station> stations = new HashMap<String, Station>();

	public static class NoSharingRunnableThread implements Runnable{

		public int start;
		public int end;
		public HashMap<String, Station> stns;

		public NoSharingRunnableThread(int start, int end, HashMap<String, Station> stns) {
			this.start = start;
			this.end = end;
			this.stns= stns;
		}

		@Override
		public void run() {
			for(int i= start; i<end;i++){
				String line = inputData.get(i);
				String[] str = line.split(",");
				if(str[2] == null || !str[2].trim().equalsIgnoreCase("TMAX")){
					continue;
				} else {
					String key = str[0];
					if(stns.containsKey(key)){
						Station stn = stns.get(key);
						int tempr = Integer.parseInt(str[3]);
						stn.addTemperatureSum(tempr);
						stn.incrementCount();
						stns.put(key, stn);
					} else {
						int tempr = Integer.parseInt(str[3]);
						Station stn = new Station(key, tempr);
						stns.put(key, stn);
					}
				}
			}	
		}
	}

	public static HashMap<String, Station> performNoSharing(ArrayList<String> input) { 
		inputData= input;
		ArrayList<HashMap<String, Station>> noSharingList = new ArrayList<>();
		int processors = Runtime.getRuntime().availableProcessors();
		ExecutorService executor = Executors.newFixedThreadPool(processors);
		int start = 0;
		int end=0;
		for(int i=0; i<processors; i++){
			noSharingList.add(new HashMap<String, Station>());
		}
		for(int i= 0; i<processors; i++){
			end += (inputData.size() - start)/(processors - i);
			executor.execute(new NoSharingRunnableThread(start, end, noSharingList.get(i)));
			start = end;
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}

		for(HashMap<String,Station> statn : noSharingList){
			for(String key: statn.keySet()){
				if(stations.containsKey(key)){
					Station stn = stations.get(key);
					int temperatureSum = statn.get(key).getTemperatureSum();
					int temperatureCount = statn.get(key).getTemperatureCount();
					stn.aggregateStation(temperatureSum, temperatureCount);
				} else {
					Station stn = new Station(key, statn.get(key).getTemperatureSum(), 
							statn.get(key).getTemperatureCount());
					stations.put(key, stn);
				}
			}
		}
		return stations;
	}
}
