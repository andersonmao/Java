package data;

import java.util.List;
import java.util.Map;

import data.DataColumn.DataColumnCriteria;
import data.DataColumn.DataColumnSort;

/**
 * @author Anderson Mao, 2014-06-11
 */
public interface DataCache {
	/**
	 * Will delete all previous data for this tableName
	 * @param tableName: unique name for the whole app.
	 * For same business unit, client can add user id to make it unique. Example: LV_0001 (LV is unit, 0001 is user id) 
	 */
	void set(String tableName, String idColumnName, List<DataColumn> columnList, List<Map<String, String> > rowList) throws DataException;
	
	/**
	 * Get one row
	 */
	Map<String, String> get(String tableName, String id) throws DataException;
	
	/**
	 * Get page rows
	 * @param criteria: null to get all rows
	 * @param sort: null to sort by id
	 * @param columnNameList: null to get all columns
	 */
	List<Map<String, String> > get(String tableName, int pageSize, int pageNumber, DataColumnCriteria criteria, DataColumnSort sort, List<String> columnNameList) throws DataException;
	
	/**
	 * Get page rows by SQL
	 * @param sort: null to sort by id
	 * @param columnNameList: null to get all columns
	 */
	List<Map<String, String> > get(String tableName, int pageSize, int pageNumber, String sql, DataColumnSort sort, List<String> columnNameList) throws DataException;
	
	/**
	 * Get count for query criteria
	 * @param criteria: null to get all
	 */
	int count(String tableName, DataColumnCriteria criteria) throws DataException;
	
	/**
	 * Get count for query SQL
	 * @param sql: null to get all
	 */
	int count(String tableName, String sql) throws DataException;
}
