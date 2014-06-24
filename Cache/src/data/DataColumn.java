package data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Anderson Mao, 2014-06-13
 */
public class DataColumn {
	private static final SimpleDateFormat ISO_DATE_FORMAT_DAY    = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat ISO_DATE_FORMAT_HOUR   = new SimpleDateFormat("yyyy-MM-dd HH");
	private static final SimpleDateFormat ISO_DATE_FORMAT_MINUTE = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	private static final SimpleDateFormat ISO_DATE_FORMAT_SECOND = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final List<SimpleDateFormat> DATE_FORMAT_LIST = new ArrayList<SimpleDateFormat>();
	static{
		// The sequence is important. The parser will try one by one
		DATE_FORMAT_LIST.add(ISO_DATE_FORMAT_SECOND);
		DATE_FORMAT_LIST.add(ISO_DATE_FORMAT_MINUTE);
		DATE_FORMAT_LIST.add(ISO_DATE_FORMAT_HOUR);
		DATE_FORMAT_LIST.add(ISO_DATE_FORMAT_DAY);
	}
	
	private String name;
	private DataColumnType type;
	
	public DataColumn(String name, DataColumnType type){
		this.name = name.toUpperCase(); // [USE_UPPER_CASE_COLUMN_NAME]
		this.type = type;
	}
	
	public String getName(){
		return name;
	}
	
	public DataColumnType getType(){
		return type;
	}
	
	/**
	 * Convert input Object to internal Object according to @param type
	 * @see DataColumnType
	 * @param value: Any type of Object from input
	 */
	public static Object convertDataColumnValue(DataColumnType type, Object value){
		if(value==null){
			return null;
		}
		Object myValue = value.toString();
		if(DataColumnType.INTEGER.equals(type) ){
			if(!(value instanceof Integer) ){
				myValue = Integer.valueOf(value.toString() );
			}
		}else if(DataColumnType.LONG.equals(type) ){
			if(!(value instanceof Long) ){
				myValue = Long.valueOf(value.toString() );
			}
		}else if(DataColumnType.DOUBLE.equals(type) ){
			if(!(value instanceof Double) ){
				myValue = Double.valueOf(value.toString() );
			}
		}else if(DataColumnType.DATE.equals(type) ){
			myValue = parseDate(value);
		}
		//
		return myValue;
	}
	
	/**
	 * Convert string to Object according to @param type
	 * @see DataColumnType
	 */
	public static Object parseDataColumnValue(DataColumnType type, String value){
		if(value == null){
			return null;
		}
		Object myValue = value;
		if(DataColumnType.INTEGER.equals(type) ){
			myValue = Integer.valueOf(value);
		}else if(DataColumnType.LONG.equals(type) ){
			myValue = Long.valueOf(value);
		}else if(DataColumnType.DOUBLE.equals(type) ){
			myValue = Double.valueOf(value);
		}else if(DataColumnType.DATE.equals(type) ){
			myValue = parseDate(value);
		}
		//
		return myValue;
	}
	
	private static Long parseDate(Object value){
		Long myValue = null;
		if(value instanceof Long){
			myValue = (Long) value;
		}else if(value instanceof Date){
			myValue = ((Date)value).getTime();
		}else{
			String s = value.toString();
			Date date = null;
			for(SimpleDateFormat format: DATE_FORMAT_LIST){
				try{
					date = format.parse(s);
					break;
				}catch(ParseException ex){
					// continue
				}
			}
			if(date != null){
				myValue = date.getTime();
			}else{
				try{
					myValue = Long.valueOf(value.toString() );
				}catch(NumberFormatException ex){
					throw new DataException("Can not parse Date value from '"+value.toString()+"'");
				}
			}
		}
		return myValue;
	}
	
	public static enum DataColumnType{
		INTEGER,
		LONG,
		DOUBLE,
		STRING,
		DATE // Date will use long timestamp value
	}
	
	public static enum DataColumnOperator{
		// Both Number/Date and String operations
		EQ,
		NOT_EQ,
		// Number/Date operations
		GE,
		LE,
		GT,
		LT,
		// String operations
		LIKE,
		NOT_LIKE;
	}
	
	protected static class DataColumnCriterion{
		public String columnName;
		public DataColumnOperator operator;
		public String value;
	
		public DataColumnCriterion(String columnName, DataColumnOperator operator, String value){
			this.columnName = columnName.toUpperCase();// [USE_UPPER_CASE_COLUMN_NAME]
			this.operator = operator;
			this.value = value;
		}
		
		@Override
		public String toString(){
			return columnName+"_"+operator.name()+"_"+value;
		}
	}
	
	public static class DataColumnCriteria{
		List<DataColumnCriterion> criterionList = new ArrayList<DataColumnCriterion>();
		public DataColumnCriteria add(String columnName, DataColumnOperator operator, String value){
			DataColumnCriterion c = new DataColumnCriterion(columnName, operator, value);
			criterionList.add(c);
			return this;
		}
		
		public DataColumnCriteria add(DataColumnCriterion criterion){
			if(criterion !=null){
				criterionList.add(criterion);
			}
			return this;
		}
		
		protected List<DataColumnCriterion> getCriterionList(){
			return criterionList;
		}
	}
	
	public static class DataColumnSortColumn{
		public String columnName;
		public Boolean columnAsc;
	
		public DataColumnSortColumn(String columnName, Boolean columnAsc){
			this.columnName = columnName.toUpperCase();// [USE_UPPER_CASE_COLUMN_NAME]
			this.columnAsc = columnAsc;
		}
		
		@Override
		public String toString(){
			return columnName+"_"+columnAsc;
		}
	}
	
	public static class DataColumnSort{
		private List<DataColumnSortColumn> sortColumnList = new ArrayList<DataColumnSortColumn>();
		
		public DataColumnSort add(String columnName, Boolean columnAsc){
			if(columnName != null){
				DataColumnSortColumn sortColumn = new  DataColumnSortColumn(columnName, columnAsc);
				sortColumnList.add(sortColumn);
			}
			return this;
		}
		
		public DataColumnSort add(DataColumnSortColumn sortColumn){
			sortColumnList.add(sortColumn);
			return this;
		}
		
		public List<DataColumnSortColumn> getSortColumnList(){
			return sortColumnList;
		}
		
		public DataColumnSort(){
		}
		
		public DataColumnSort(String columnName, Boolean columnAsc){
			if(columnName != null){
				DataColumnSortColumn sortColumn = new  DataColumnSortColumn(columnName, columnAsc);
				sortColumnList.add(sortColumn);
			}
		}
		
		public DataColumnSort(String columnName, Boolean columnAsc, String secondColumnName, Boolean secondColumnAsc){
			if(columnName != null){
				DataColumnSortColumn sortColumn = new  DataColumnSortColumn(columnName, columnAsc);
				sortColumnList.add(sortColumn);
			}
			if(secondColumnName !=null){
				DataColumnSortColumn sortColumn = new  DataColumnSortColumn(secondColumnName, secondColumnAsc);
				sortColumnList.add(sortColumn);
			}
		}
		
		@Override
		public String toString(){
			StringBuilder sb = new StringBuilder();
			for(DataColumnSortColumn sortColumn: sortColumnList){
				sb.append(sortColumn.toString() );
			}
			return sb.toString();
		}
	}
}
