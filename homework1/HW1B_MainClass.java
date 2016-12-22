package homework1;

import java.util.ArrayList;
import java.util.HashMap;

public class HW1B_MainClass {

	//Assign the variable with the path to the input file
	public static String inputFileName = "1912.csv";

	//Data structure to create 1-1 memory copy of the input file
	public static ArrayList<String> inputData = new ArrayList<String>();

	//Hash map with key as stationID and value with the station object
	public static HashMap<String, Station> stations = new HashMap<String, Station>();

	// DataStructure to store time taken using Sequential
	public static long sequentialMinTime = Long.MAX_VALUE;
	public static long sequentialMaxTime = Long.MIN_VALUE;
	public static long sequentialTotalTime = 0;

	// DataStructure to store time taken using NoLock
	public static long noLockMinTime = Long.MAX_VALUE;
	public static long noLockMaxTime = Long.MIN_VALUE;
	public static long noLockTotalTime = 0;

	// DataStructure to store time taken using CoarseLock
	public static long coarseLockMinTime = Long.MAX_VALUE;
	public static long coarseLockMaxTime = Long.MIN_VALUE;
	public static long coarseLockTotalTime = 0;

	// DataStructure to store time taken using FineLock
	public static long fineLockMinTime = Long.MAX_VALUE;
	public static long fineLockMaxTime = Long.MIN_VALUE;
	public static long fineLockTotalTime = 0;

	// DataStructure to store time taken using No Sharing
	public static long noSharingMinTime = Long.MAX_VALUE;
	public static long noSharingMaxTime = Long.MIN_VALUE;
	public static long noSharingTotalTime = 0;

	//Time Computations for Sequential
	public static void sequentialTimeComputation(long startTime, long endTime){
		long timeTaken = endTime - startTime;
		sequentialTotalTime += timeTaken;
		if (timeTaken < sequentialMinTime) {
			sequentialMinTime = timeTaken;
		}
		if (timeTaken > sequentialMaxTime) {
			sequentialMaxTime = timeTaken;
		}
	}

	//Time Computations for No Lock
	public static void noLockTimeComputation(long startTime, long endTime){
		long timeTaken = endTime - startTime;
		noLockTotalTime += timeTaken;
		if (timeTaken < noLockMinTime) {
			noLockMinTime = timeTaken;
		}
		if (timeTaken > noLockMaxTime) {
			noLockMaxTime = timeTaken;
		}
	}

	//Time Computations for Coarse Lock
	public static void coarseLockTimeComputation(long startTime, long endTime){
		long timeTaken = endTime - startTime;
		coarseLockTotalTime += timeTaken;
		if (timeTaken < coarseLockMinTime) {
			coarseLockMinTime = timeTaken;
		}
		if (timeTaken > coarseLockMaxTime) {
			coarseLockMaxTime = timeTaken;
		}
	}

	//Time Computations for FineLock
	public static void fineLockTimeComputation(long startTime, long endTime){
		long timeTaken = endTime - startTime;
		fineLockTotalTime += timeTaken;
		if (timeTaken < fineLockMinTime) {
			fineLockMinTime = timeTaken;
		}
		if (timeTaken > fineLockMaxTime) {
			fineLockMaxTime = timeTaken;
		}
	}

	//Time Computations for No Sharing
	public static void noSharingTimeComputation(long startTime, long endTime){
		long timeTaken = endTime - startTime;
		noSharingTotalTime += timeTaken;
		if (timeTaken < noSharingMinTime) {
			noSharingMinTime = timeTaken;
		}
		if (timeTaken > noSharingMaxTime) {
			noSharingMaxTime = timeTaken;
		}
	}

	public static void main(String[] args) {
		inputData= Loader.loadInputFile(inputFileName);
		System.out.println("Done with loading input data");

		for(int i=0; i< 10; i++){
			long startTime;
			long endTime;
			
			startTime= System.currentTimeMillis();
			Sequential.performSequential(inputData);
			endTime= System.currentTimeMillis();
			sequentialTimeComputation(startTime, endTime);
			
			startTime= System.currentTimeMillis();
			NoLock.performNoLock(inputData);
			endTime= System.currentTimeMillis();
			noLockTimeComputation(startTime, endTime);
			
			startTime= System.currentTimeMillis();
			CoarseLock.performCoarseLock(inputData);
			endTime= System.currentTimeMillis();
			coarseLockTimeComputation(startTime, endTime);
			
			startTime= System.currentTimeMillis();
			FineLock.performFineLock(inputData);
			endTime= System.currentTimeMillis();
			fineLockTimeComputation(startTime, endTime);
			
			startTime= System.currentTimeMillis();
			NoSharing.performNoSharing(inputData);
			endTime= System.currentTimeMillis();
			noSharingTimeComputation(startTime, endTime);
		}
		
		System.out.println("Maximum time taken for Sequential: " + sequentialMaxTime);
		System.out.println("Minimum time taken for Sequential: " + sequentialMinTime);
		System.out.println("Average time taken for Sequential: " + sequentialTotalTime/10);
		
		System.out.println("Maximum time taken for No Lock: " + noLockMaxTime);
		System.out.println("Minimum time taken for No Lock: " + noLockMinTime);
		System.out.println("Average time taken for No Lock: " + noLockTotalTime/10);
		
		System.out.println("Maximum time taken for Coarse Lock: " + coarseLockMaxTime);
		System.out.println("Minimum time taken for Coarse Lock: " + coarseLockMinTime);
		System.out.println("Average time taken for Coarse Lock: " + coarseLockTotalTime/10);
		
		System.out.println("Maximum time taken for Fine Lock: " + fineLockMaxTime);
		System.out.println("Minimum time taken for Fine Lock: " + fineLockMinTime);
		System.out.println("Average time taken for Fine Lock: " + fineLockTotalTime/10);
		
		System.out.println("Maximum time taken for No Sharing: " + noSharingMaxTime);
		System.out.println("Minimum time taken for No Sharing: " + noSharingMinTime);
		System.out.println("Average time taken for No Sharing: " + noSharingTotalTime/10);

	}

}
