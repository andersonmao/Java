package data;

/**
 * @author Anderson Mao, 2014-06-11
 */
public class DataException extends RuntimeException {
	private static final long serialVersionUID = -1;

	public DataException(){
	}
	
	public DataException(String message){
		super(message);
	}
	
	public DataException(Throwable throwable){
		super(throwable);
	}
}
