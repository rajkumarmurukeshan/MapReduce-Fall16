package homework1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NoLock {

	//Data structure that contains the 1-1 memory copy of the input data
	public static ArrayList<String> inputData = new ArrayList<String>();

	//Hash map with key as stationID and value with the station object
	public static HashMap<String, Station> stations = new HashMap<String, Station>();

	public static class NoLockRunnableThread implements Runnable{

		public int start;
		public int end;

		public NoLockRunnableThread(int start, int end) {
			this.start = start;
			this.end = end;
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

	}


	public static HashMap<String, Station> performNoLock(ArrayList<String> input) { 
		inputData= input;
		int processors = Runtime.getRuntime().availableProcessors();
		ExecutorService executor = Executors.newFixedThreadPool(processors);
		int start = 0;
		int end=0;
		for(int i= 0; i<processors; i++){
			end += (inputData.size() - start)/(processors - i);
			executor.execute(new NoLockRunnableThread(start, end));
			start = end;
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		return stations;
	}

}
