package data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import data.DataColumn.DataColumnType;

/**
 * @author Anderson Mao, 2014-06-13
 */
public class DataTable {
	protected String tableName    = null;
	protected String idColumnName = null;
	
	private List<DataColumn> columnList = new ArrayList<DataColumn>();
	private Map<String, DataColumnType> columnTypeMap = new HashMap<String, DataColumnType>();
	private Set<String> columnNameSet = new HashSet<String>();
	private Set<String> stringColumnNameSet = new HashSet<String>();
	
	public DataTable(String tableName, String idColumnName, List<DataColumn> columnList){
		this.tableName = tableName;
		this.idColumnName = idColumnName.toUpperCase();// [USE_UPPER_CASE_COLUMN_NAME]
		this.columnList = columnList;
		for(DataColumn c: columnList){
			columnTypeMap.put(c.getName(), c.getType() );
			columnNameSet.add(c.getName() );
			if(c.getType().equals(DataColumnType.STRING) ){
				stringColumnNameSet.add(c.getName() );
			}
		}
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public String getIdColumnName(){
		return idColumnName;
	}
	
	public boolean isColumn(String columnName){
		return columnNameSet.contains(columnName);
	}
	
	public boolean isStringColumn(String columnName){
		return stringColumnNameSet.contains(columnName);
	}
	
	public DataColumnType getColumnType(String columnName){
		if(columnName == null){
			return null;
		}
		return columnTypeMap.get(columnName);
	}
	
	public String getId(Map<String, String> row){
		return row.get(idColumnName);
	}
	
	public List<DataColumn> getColumnList(){
		return columnList;
	}
	
	public String getKey(String id){
		return tableName+":"+id;
	}
	
	/**
	 * Key for id List
	 */
	public String getIdListKey(){
		return tableName+":ID:LIST";
	}
	
	/**
	 * Key for id => column value HashMap
	 */
	public String getHashKey(String columnName){
		return tableName+":HASH:"+columnName;
	}
	
	/**
	 * Key for column value SortedSet
	 */
	public String getIndexKey(String columnName){
		return tableName+":INDEX:"+columnName;
	}

	/**
	 * Key for query name String
	 */
	public String getQueryNameKey(){
		return tableName+":QUERY:NAME";
	}
	
	/**
	 * Key for query id List
	 */
	public String getQueryListKey(){
		return tableName+":QUERY:LIST";
	}
	
	/**
	 * Key for sort name String
	 */
	public String getSortNameKey(){
		return tableName+":SORT:NAME";
	}
	
	/**
	 * Key for sort id list
	 */
	public String getSortListKey(){
		return tableName+":SORT:LIST";
	}
}
