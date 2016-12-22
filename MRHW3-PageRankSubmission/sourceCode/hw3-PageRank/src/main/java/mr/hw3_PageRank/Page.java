package mr.hw3_PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class Page implements WritableComparable<Page> {

	public String pageName;
	public Double pageRank;
	
	public Page() {
		super();
		this.pageName = "";
		this.pageRank = 0.00;
	}
	
	public Page(String pageName, Double pageRank) {
		super();
		this.pageName = pageName;
		this.pageRank = pageRank;
	}
	
	public String getPageName() {
		return pageName;
	}
	public void setPageName(String pageName) {
		this.pageName = pageName;
	}
	public Double getPageRank() {
		return pageRank;
	}
	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}
	
	public void readFields(DataInput arg0) throws IOException {
		pageName = WritableUtils.readString(arg0);
        pageRank = Double.parseDouble(WritableUtils.readString(arg0));	
	}
	
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeString(arg0, pageName);
        WritableUtils.writeString(arg0, "" + pageRank);
		
	}
	
	public int compareTo(Page o) {
		return o.getPageRank().compareTo(this.pageRank);
	}
}
