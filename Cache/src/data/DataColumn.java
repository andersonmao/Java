package data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Anderson Mao, 2014-06-13
 */
public class DataColumn {
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
	
	public static enum DataColumnType{
		LONG, DOUBLE, STRING, DATE
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
	
	public static class DataColumnSort{
		public String columnName;
		public boolean columnAsc;
		public String secondColumnName;
		public boolean secondColumnAsc;
		
		public DataColumnSort(String columnName, boolean columnAsc){
			this.columnName = columnName;
			if(this.columnName != null){// [USE_UPPER_CASE_COLUMN_NAME]
				this.columnName = this.columnName.toUpperCase();
			}
			this.columnAsc = columnAsc;
		}
		
		public DataColumnSort(String columnName, boolean columnAsc, String secondColumnName, boolean secondColumnAsc){
			this.columnName = columnName;
			this.columnAsc = columnAsc;
			this.secondColumnName = secondColumnName;
			this.secondColumnAsc = secondColumnAsc;
			if(this.columnName != null){// [USE_UPPER_CASE_COLUMN_NAME]
				this.columnName = this.columnName.toUpperCase();
			}
			if(this.secondColumnName != null){// [USE_UPPER_CASE_COLUMN_NAME]
				this.secondColumnName = this.secondColumnName.toUpperCase();
			}
		}
		
		@Override
		public String toString(){
			return columnName+"_"+columnAsc+"_"+secondColumnName+"_"+secondColumnAsc;
		}
	}
}
