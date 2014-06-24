package data;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import data.DataColumn.DataColumnCriterion;
import data.DataColumn.DataColumnOperator;
import data.DataException;
import data.DataSql;
import data.DataSql.DataSqlNode;
import data.DataSql.DataSqlOperator;

/**
 * @author Anderson Mao, 2014-06-23
 */
public class DataSqlTest extends TestCase{
	
	@Override
	public void setUp(){
		Logger.getRootLogger().setLevel(Level.DEBUG);
	}
	
	@Test
	public void testParseSqlNull(){
		String sql = null;
		DataSqlNode node = DataSql.parseSql(sql);
		assertNull(node);
	}
	
	@Test
	public void testParseSqlOne(){
		String sql = "age > 10";
		DataSqlNode node = DataSql.parseSql(sql);
		assertNotNull(node);
		assertNotNull(node.criterion);
		assertEquals("AGE", node.criterion.columnName);
		assertEquals(DataColumnOperator.GT, node.criterion.operator);
		assertEquals("10", node.criterion.value);
	}
	
	@Test
	public void testParseSqlException(){
		String sql = "age >";
		DataException dataEx = null;
		try{
			DataSql.parseSql(sql);
		}catch(DataException ex){
			dataEx = ex;
		}
		assertNotNull(dataEx);
	}
	
	@Test
	public void testParseSqlNotEq(){
		String sql = "name <> 'a'";
		DataSqlNode node = DataSql.parseSql(sql);
		assertEquals(DataColumnOperator.NOT_EQ, node.criterion.operator);
		//
		String sqlNe = "name != 'a'";
		DataSqlNode nodeNe = DataSql.parseSql(sqlNe);
		assertEquals(DataColumnOperator.NOT_EQ, nodeNe.criterion.operator);
	}
	
	@Test
	public void testParseSqlNotLike(){
		String sql = "name not like 'a'";
		DataSqlNode node = DataSql.parseSql(sql);
		assertEquals(DataColumnOperator.NOT_LIKE, node.criterion.operator);
	}
	
	@Test
	public void testParseSqlAnd(){
		String sql = "age > 10 and name like 'a'";
		DataSqlNode node = DataSql.parseSql(sql);
		assertNotNull(node);
		assertNull(node.criterion);
		assertEquals(DataSqlOperator.AND, node.operator);
		assertEquals("AGE", node.leftNode.criterion.columnName);
		assertEquals("NAME", node.rightNode.criterion.columnName);
	}
	
	@Test
	public void testParseSqlOr(){
		String sql = "age > 10 or name like 'a'";
		DataSqlNode node = DataSql.parseSql(sql);
		assertNotNull(node);
		assertNull(node.criterion);
		assertEquals(DataSqlOperator.OR, node.operator);
	}
	
	@Test
	public void testParseSqlNest(){
		String sql = "age > 10 and (name like 'a' or age < 20)";
		DataSqlNode node = DataSql.parseSql(sql);
		assertNotNull(node);
		assertNull(node.criterion);
		assertEquals(DataSqlOperator.AND, node.operator);
		assertEquals(DataSqlOperator.OR, node.rightNode.operator);
	}
	
	@Test
	public void testParseSqlSepWithoutSpace(){
		String sql = "age>10 and age< =20";
		DataSqlNode node = DataSql.parseSql(sql);
		assertNotNull(node);
		assertEquals(DataSqlOperator.AND, node.operator);
		assertEquals(DataColumnOperator.GT, node.leftNode.criterion.operator);
		assertEquals(DataColumnOperator.LE, node.rightNode.criterion.operator);
	}
	
	@Test
	public void testCreateNodeAnd(){
		List<DataColumnCriterion> criterionList = new ArrayList<DataColumnCriterion>();
		criterionList.add(new DataColumnCriterion("", DataColumnOperator.EQ, "") );
		criterionList.add(new DataColumnCriterion("", DataColumnOperator.EQ, "") ); 
		criterionList.add(new DataColumnCriterion("", DataColumnOperator.EQ, "") );
		DataSqlNode node = DataSql.createNode(criterionList, DataSqlOperator.AND);
		assertEquals(DataSqlOperator.AND, node.operator);
		assertEquals(DataSqlOperator.AND, node.leftNode.operator);
	}
	
	@Test
	public void testCreateNodeOr(){
		List<DataColumnCriterion> criterionList = new ArrayList<DataColumnCriterion>();
		criterionList.add(new DataColumnCriterion("", DataColumnOperator.EQ, "") );
		criterionList.add(new DataColumnCriterion("", DataColumnOperator.EQ, "") );
		criterionList.add(new DataColumnCriterion("", DataColumnOperator.EQ, "") );
		DataSqlNode node = DataSql.createNode(criterionList, DataSqlOperator.OR);
		assertEquals(DataSqlOperator.OR, node.operator);
		assertEquals(DataSqlOperator.OR, node.leftNode.operator);
	}
}
