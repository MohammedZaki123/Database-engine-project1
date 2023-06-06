package engine;
public class Attribute{
	private String name;
	private String datatype;
	private String min;
	private String max;
	boolean is_Clustering_key;
	private int table_location;
	public Attribute(String name,String datatype,String min,String max){
		this.name = name;
		this.datatype = datatype;
		this.min = min;
		this.max = max;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDatatype() {
		return datatype;
	}
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	public String getMin() {
		return min;
	}
	public void setMin(String min) {
		this.min = min;
	}
	public String getMax() {
		return max;
	}
	public void setMax(String max) {
		this.max = max;
	}
	public int getTable_location() {
		return table_location;
	}
	public void setTable_location(int table_location) {
		this.table_location = table_location;
	}
}
