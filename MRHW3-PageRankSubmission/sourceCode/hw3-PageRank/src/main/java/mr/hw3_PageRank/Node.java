package mr.hw3_PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Node implements Writable {
	
	public String[] adjacencyList;
	public Double pageRank;
	public boolean isContribution;
	
	public Node(){
		this.adjacencyList= new String[0];
		this.pageRank= 0.00;
		this.isContribution= false;
	}
	
	public Node(String[] adjacencyList, Double pageRank) {
		super();
		this.adjacencyList= adjacencyList;
		this.pageRank= pageRank;
		this.isContribution= false;
	}
	

	public Node(Double pageRank) {
		super();
		this.adjacencyList = new String[] {};
		this.pageRank = pageRank;
		this.isContribution = true;
	}

	public boolean isContribution() {
		return isContribution;
	}

	public void setContribution(boolean isContribution) {
		this.isContribution = isContribution;
	}

	public String[] getAdjacencyList() {
		return adjacencyList;
	}

	public void setAdjacencyList(String[] adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public Double getPageRank() {
		return pageRank;
	}

	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}

	public void readFields(DataInput arg0) throws IOException {
		adjacencyList = WritableUtils.readStringArray(arg0);
        pageRank = Double.parseDouble(WritableUtils.readString(arg0));
        isContribution = Boolean.parseBoolean(WritableUtils.readString(arg0));
	}

	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeStringArray(arg0, adjacencyList);
        WritableUtils.writeString(arg0, "" + pageRank.toString());
        WritableUtils.writeString(arg0, "" + isContribution);
	}

}
