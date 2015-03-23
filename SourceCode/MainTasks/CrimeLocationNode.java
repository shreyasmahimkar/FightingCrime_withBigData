

public class CrimeLocationNode {
	
	String streetName, city, state;
	String zipCode;
	final String delimiter = ",";
	
	CrimeLocationNode(String streetName,String city,String state,String zipCode){
		this.streetName = streetName.toLowerCase();
		this.city = city.toLowerCase();
		this.state = state.toLowerCase();
		this.zipCode = zipCode.toLowerCase();
	}
	
	public String createTextForNode(){
		StringBuilder buildString = new StringBuilder();
		buildString.append(this.streetName+delimiter);
		buildString.append(this.city+delimiter);
		buildString.append(this.state+delimiter);
		buildString.append(this.zipCode);
		
		return buildString.toString();
	}
}
