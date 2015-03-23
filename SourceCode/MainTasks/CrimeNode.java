

public class CrimeNode{
	private String crimeType;
	private int countOfCrime;
	private String allTimesOfCrime;
	final String delimiter = ",";
	final String timeDelimiter = "$";
	
	
	CrimeNode(String crimeType, String timesOfCrime) {
		this.setCountOfCrime(1);
		this.setCrimeType(crimeType);
		this.setAllTimesOfCrime(timesOfCrime);
	}
	
	public void addCount(){
		this.setCountOfCrime(this.countOfCrime + 1);
	}
	
	public void addTime(String timeToAdd){
		this.setAllTimesOfCrime(this.getAllTimesOfCrime() + timeDelimiter + timeToAdd); 
	}
	
	public String createTextForNode(){
		StringBuilder buildString = new StringBuilder();
		buildString.append(this.getCrimeType()+delimiter);
		buildString.append(String.valueOf(this.getCountOfCrime())+delimiter);
		buildString.append(this.allTimesOfCrime);
		
		return buildString.toString();
	}
	
	// ------------------- GETTER SETTERS --------------------- //
	public int getCountOfCrime() {
		return countOfCrime;
	}

	public void setCountOfCrime(int countOfCrime) {
		this.countOfCrime = countOfCrime;
	}
	
	public String getCrimeType() {
		return crimeType;
	}

	public void setCrimeType(String crimeType) {
		this.crimeType = crimeType;
	}

	public String getAllTimesOfCrime() {
		return allTimesOfCrime;
	}

	public void setAllTimesOfCrime(String allTimesOfCrime) {
		this.allTimesOfCrime = allTimesOfCrime;
	}
	// ------------------------------------------------------------------//
	
	public String toString() {
		return this.getCrimeType().toString() + delimiter + this.getCountOfCrime() + delimiter + this.getAllTimesOfCrime();
	};
}