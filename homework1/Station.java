package homework1;


public class Station {
	public String stationId;
	public int temperatureSum;
	public int temperatureCount;

	public Station(String stationId, int temperature){
		this.stationId = stationId;
		this.temperatureSum = temperature;
		this.temperatureCount = 1;
	}
	
	public Station(String stationId, int temperatureSum, int temperatureCount){
		this.stationId = stationId;
		this.temperatureSum = temperatureSum;
		this.temperatureCount = temperatureCount;
	}
	
	public void addTemperatureSum(int temperature){
		fibonacci(17);
		this.temperatureSum+= temperature;
	}
	
	public void incrementCount(){
		this.temperatureCount++;
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public int getTemperatureSum() {
		return temperatureSum;
	}

	public void setTemperatureSum(int temperatureSum) {
		this.temperatureSum = temperatureSum;
	}
	
	public void aggregateStation(int temperatureSum, int temperatureCount){
		this.temperatureSum += temperatureSum;
		this.temperatureCount += temperatureCount;
	}

	public int getTemperatureCount() {
		return temperatureCount;
	}

	public void setTemperatureCount(int temperatureCount) {
		this.temperatureCount = temperatureCount;
	}
	
	public int fibonacci(int n)
    {
        if(n == 0)
            return 0;
        else if(n == 1)
            return 1;
        else
            return fibonacci(n - 1) + fibonacci(n - 2);
    }
}
