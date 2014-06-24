package data;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import data.DataColumn.DataColumnCriteria;
import data.DataColumn.DataColumnCriterion;
import data.DataColumn.DataColumnOperator;
import data.DataColumn.DataColumnSort;
import data.DataColumn.DataColumnSortColumn;
import data.DataColumn.DataColumnType;
import data.DataSql.DataSqlNode;
import data.DataSql.DataSqlOperator;

/**
 * 
 * @author Anderson Mao, 2014-06-19
 */
public class DataCacheMongoImpl implements DataCache {
	private static final Logger logger = Logger.getLogger(DataCacheMongoImpl.class);
	private static void debug(String message){
		if(Logger.getRootLogger().getAllAppenders().hasMoreElements() ){
			logger.debug(message);
		}else{
			System.out.println((new Date()).toString()+": "+DataCacheMongoImpl.class.getSimpleName()+": "+message);
		}
	}
	
	private MongoTemplate mongoTemplate = null;
	public void setMongoTemplate(MongoTemplate mongoTemplate){
		this.mongoTemplate = mongoTemplate;
	}
	
	/** Prefer batch size [100, 1000] */
	private static final int INSERT_BATCH_SIZE = 1000;
	
	/** Collection name used to save DataTable */
	private static final String DATA_TABLE_COLLECTION_NAME = "DATA_CACHE_DATA_TABLE";
	private static final String DATA_TABLE_JSON_KEY_NAME   = "VALUE";
	private static final Type   DATA_TABLE_TYPE = new TypeToken<DataTable >(){}.getType();
	/** Use GSON to save DataTable */
	private Gson gson = new Gson();
	
	private DBCollection getCollection(String name){
		return mongoTemplate.getCollection(name);
	}

	@Override
	public void set(String tableName, String idColumnName, List<DataColumn> columnList, List<Map<String, Object>> rowList) throws DataException {
		debug("set: begin: table="+tableName);
		try{
			DataTable dataTable = new DataTable(tableName, idColumnName, columnList);
			saveDataTable(dataTable);
			// Reset
			DBCollection coll = getCollection(tableName);
			coll.drop();
			// Add all
			debug("set: insert");
			int rowListSize = rowList.size();
			int stepSize = rowListSize / 10;
			List<DBObject> batchList = new ArrayList<DBObject>(INSERT_BATCH_SIZE);
			for(int i=0; i< rowListSize; i++){
				Map<String, Object> row = rowList.get(i);
				// [USE_UPPER_CASE_COLUMN_NAME]
				row = DataUtil.toUpperCase(row);
				Map<String, Object> myRow = new HashMap<String, Object>();
				for(DataColumn column: dataTable.getColumnList() ){
					DataColumnType type = column.getType();
					String columnName = column.getName();
					Object value = row.get(columnName);
					if(value !=null){
						Object myValue = DataColumn.convertDataColumnValue(type, value);
						myRow.put(columnName, myValue);
					}
				}
				//
				String id = dataTable.getId(row);
				BasicDBObject obj = new BasicDBObject();
				obj.putAll(myRow);
				obj.put("_id", id);
				// Use batch insert instead of insert one by one
				batchList.add(obj);
				if(batchList.size() >= INSERT_BATCH_SIZE){
					coll.insert(batchList);
					batchList = new ArrayList<DBObject>(INSERT_BATCH_SIZE);
				}
				// Show step when run multi batch
				if(stepSize >= INSERT_BATCH_SIZE && ((i+1) % stepSize == 0) ){
					debug("set: insert ["+(i+1)+"/"+rowListSize+"]");
				}
			}
			// Last batch
			if(batchList.size() > 0){
				coll.insert(batchList);
				batchList = null;
			}
			// Create indexes
			debug("set: index");
			for(DataColumn column: dataTable.getColumnList() ){
				String columnName = column.getName();
				if(!dataTable.isStringColumn(columnName) ){
					DBObject indexObj = new BasicDBObject();
					indexObj.put(columnName, 1);
					coll.createIndex(indexObj);
				}
			}
		}catch(MongoException ex){
			throw new DataException(ex);
		}catch(NumberFormatException ex){
			throw new DataException(ex);
		}
	}

	@Override
	public Map<String, Object> get(String tableName, String id) throws DataException {
		Map<String, Object> row = null;
		try{
			DataTable dataTable = getDataTable(tableName);
			DBCollection coll = getCollection(tableName);
			BasicDBObject queryObj = new BasicDBObject();
			queryObj.put("_id", id);
			DBObject obj =coll.findOne(queryObj);
			row = getRow(dataTable, obj, null);
		}catch(MongoException ex){
			throw new DataException(ex);
		}
		return row;
	}

	@Override
	public List<Map<String, Object>> get(String tableName, int pageSize, int pageNumber, DataColumnCriteria criteria, DataColumnSort sort, List<String> columnNameList) throws DataException {
		DataSqlNode dataSqlNode = null;
		if(criteria != null){
			dataSqlNode = DataSql.createNode(criteria.getCriterionList(), DataSqlOperator.AND);
		}
		return get(tableName, pageSize, pageNumber, dataSqlNode, sort, columnNameList);
	}

	@Override
	public List<Map<String, Object>> get(String tableName, int pageSize, int pageNumber, String sql, DataColumnSort sort, List<String> columnNameList) throws DataException {
		DataSqlNode dataSqlNode = DataSql.parseSql(sql);
		return get(tableName, pageSize, pageNumber, dataSqlNode, sort, columnNameList);
	}

	@Override
	public int count(String tableName, DataColumnCriteria criteria) throws DataException {
		DataSqlNode dataSqlNode = null;
		if(criteria != null){
			dataSqlNode = DataSql.createNode(criteria.getCriterionList(), DataSqlOperator.AND);
		}
		return count(tableName, dataSqlNode);
	}

	@Override
	public int count(String tableName, String sql) throws DataException {
		DataSqlNode dataSqlNode = DataSql.parseSql(sql);
		return count(tableName, dataSqlNode);
	}
	
	private List<Map<String, Object>> get(String tableName, int pageSize, int pageNumber, DataSqlNode dataSqlNode, DataColumnSort sort, List<String> columnNameList) throws DataException {
		List<Map<String, Object> > rowList = new ArrayList<Map<String, Object> >();
		try{
			DataTable dataTable = getDataTable(tableName);
			DBCollection coll = getCollection(tableName);
			DBObject queryObject = getSqlNodeQueryObject(dataTable, dataSqlNode);
			if(queryObject!=null){
				debug("get: query: "+queryObject.toString() );
			}
			// Column List
			columnNameList = DataUtil.toUpperCase(columnNameList);// [USE_UPPER_CASE_COLUMN_NAME]
			DBObject columnListObject = null;
			if(columnNameList!=null && columnNameList.size()>0){
				columnListObject = new BasicDBObject();
				for(String columnName: columnNameList){
					if(dataTable.isColumn(columnName) ){
						columnListObject.put(columnName, 1);
					}
				}
			}
			// Run
			DBCursor dbCursor = coll.find(queryObject, columnListObject);
			// Sort
			if(sort!=null){
				BasicDBObject sortObject = new BasicDBObject();
				for(DataColumnSortColumn sortColumn: sort.getSortColumnList() ){
					if(dataTable.isColumn(sortColumn.columnName) ){
						sortObject.append(sortColumn.columnName, (!Boolean.FALSE.equals(sortColumn.columnAsc) ) ? 1 : -1);
					}
				}
				debug("get: sort: "+sortObject);
				dbCursor.sort(sortObject);
			}
			// Page
			int pageStart = pageNumber * pageSize;
			pageStart = Math.max(pageStart, 0);
			dbCursor.skip(pageStart);
			dbCursor.limit(pageSize);
			// Get Result
			while(dbCursor.hasNext() ){
				DBObject obj = dbCursor.next();
				rowList.add(getRow(dataTable, obj, columnNameList) );
			}
		}catch(MongoException ex){
			throw new DataException(ex);
		}
		//
		return rowList;
	}
	
	private int count(String tableName, DataSqlNode dataSqlNode) throws DataException {
		try{
			DataTable dataTable = getDataTable(tableName);
			DBCollection coll = getCollection(tableName);
			DBObject queryObject = getSqlNodeQueryObject(dataTable, dataSqlNode);
			if(queryObject==null){
				return (int)coll.count();
			}
			return (int)coll.count(queryObject);
		}catch(MongoException ex){
			throw new DataException(ex);
		}
	}
	
	private Map<String, Object> getRow(DataTable dataTable, DBObject object, List<String> columnNameList){
		Map<String, Object> row = new HashMap<String, Object>();
		if(columnNameList==null || columnNameList.size() == 0){// Get all
			for(DataColumn column: dataTable.getColumnList() ){
				String columnName = column.getName();
				Object valueObject = object.get(columnName);
				if(valueObject != null){
					row.put(columnName, valueObject);
				}
			}
		}else{
			for(String columnName: columnNameList){
				if(dataTable.isColumn(columnName) ){
					Object valueObject = object.get(columnName);
					if(valueObject != null){
						row.put(columnName, valueObject);
					}
				}
			}
		}
		//
		return row;
	}
	
	private DataTable getDataTable(String tableName) {
		try{
			DBCollection coll = getCollection(DATA_TABLE_COLLECTION_NAME);
			DBObject dataTableObject = new BasicDBObject();
			dataTableObject.put("_id",  tableName);
			dataTableObject = coll.findOne(dataTableObject);
			if(dataTableObject ==null){
				throw new DataException("Can not find DataTable definition for '"+tableName+"', please run set() first");
			}
			String dataTableJson = (String) dataTableObject.get(DATA_TABLE_JSON_KEY_NAME);
			DataTable dataTable = gson.fromJson(dataTableJson, DATA_TABLE_TYPE);
			return dataTable;
		}catch(MongoException ex){
			throw new DataException(ex);
		}
	}
	
	private void saveDataTable(DataTable dataTable){
		try{
			DBObject dataTableObject = new BasicDBObject();
			String dataTableJson = gson.toJson(dataTable);
			DBCollection coll = getCollection(DATA_TABLE_COLLECTION_NAME);
			dataTableObject.put("_id",  dataTable.tableName);
			dataTableObject.put(DATA_TABLE_JSON_KEY_NAME, dataTableJson);
			coll.save(dataTableObject);
		}catch(MongoException ex){
			throw new DataException(ex);
		}
	}
	
	private DBObject getSqlNodeQueryObject(DataTable dataTable, DataSqlNode dataSqlNode){
		DBObject object = null;
		if(dataSqlNode == null){
			return object;
		}
		if(dataSqlNode.criterion !=null){
			DataColumnCriterion criterion = dataSqlNode.criterion;
			String colName = criterion.columnName;
			if(!dataTable.isColumn(colName) ){
				throw new DataException("Invalid column '"+colName+"'");
			}
			if(dataTable.isStringColumn(colName) ){
				object = getStringQueryObject(dataTable, criterion);
			}else{
				object = getNumberQueryObject(dataTable, criterion);
			}
		}else if(DataSqlOperator.AND.equals(dataSqlNode.operator) || DataSqlOperator.OR.equals(dataSqlNode.operator) ){
			object = new BasicDBObject();  
			BasicDBList values = new BasicDBList();  
			values.add(getSqlNodeQueryObject(dataTable, dataSqlNode.leftNode) );  
			values.add(getSqlNodeQueryObject(dataTable, dataSqlNode.rightNode) );
			if(DataSqlOperator.AND.equals(dataSqlNode.operator) ){
				object.put("$and", values);
			}else{
				object.put("$or", values);
			}
		}else{
			throw new DataException("Invalid DataSqlNode: "+dataSqlNode);
		}
		return object;
	}
	
	private DBObject getStringQueryObject(DataTable dataTable, DataColumnCriterion criterion){
		DBObject object = new BasicDBObject();
		String columnName = criterion.columnName;
		DataColumnOperator op = criterion.operator;
		String value = criterion.value;
		try{
			if(DataColumnOperator.EQ.equals(op) ){
				object.put(columnName, Pattern.compile("^"+value+"$", Pattern.CASE_INSENSITIVE) );
			}else if(DataColumnOperator.NOT_EQ.equals(op) ){
				object.put(columnName, new BasicDBObject("$not", Pattern.compile("^"+value+"$", Pattern.CASE_INSENSITIVE) ) );
			}else if(DataColumnOperator.LIKE.equals(op) ){
				object.put(columnName, Pattern.compile(value, Pattern.CASE_INSENSITIVE) );
			}else if(DataColumnOperator.NOT_LIKE.equals(op) ){
				object.put(columnName, new BasicDBObject("$not", Pattern.compile(value, Pattern.CASE_INSENSITIVE) ) );
			}else{
				throw new DataException("Invalid String operator '"+op.name()+"' for column '"+columnName+"'");
			}
			return object;
		}catch(PatternSyntaxException ex){
			throw new DataException("Invalid String value '"+value+"' for column '"+columnName+"'. '"+ex.getMessage()+"'");
		}
	}
	
	private DBObject getNumberQueryObject(DataTable dataTable, DataColumnCriterion criterion){
		DBObject object = new BasicDBObject();
		String columnName = criterion.columnName;
		DataColumnOperator op = criterion.operator;
		Object value = null;
		try{
			value = DataColumn.parseDataColumnValue(dataTable.getColumnType(columnName), criterion.value);
		}catch(NumberFormatException ex){
			throw new DataException("Invalid value '"+criterion.value+"' for column '"+columnName+"' of type '"+dataTable.getColumnType(columnName)+"'");
		}
		if(DataColumnOperator.EQ.equals(op) ){
			object.put(columnName, value);
		}else if(DataColumnOperator.NOT_EQ.equals(op) ){
			object.put(columnName, new BasicDBObject("$ne",value) );
		}else if(DataColumnOperator.GE.equals(op) ){
			object.put(columnName, new BasicDBObject("$gte",value) );
		}else if(DataColumnOperator.LE.equals(op) ){
			object.put(columnName, new BasicDBObject("$lte",value) );
		}else if(DataColumnOperator.GT.equals(op) ){
			object.put(columnName, new BasicDBObject("$gt",value) );
		}else if(DataColumnOperator.LT.equals(op) ){
			object.put(columnName, new BasicDBObject("$lt",value) );
		}else{
			throw new DataException("Invalid operator '"+op+"' for column '"+columnName+"' of type '"+dataTable.getColumnType(columnName)+"'");
		}
		return object;
	}
}
