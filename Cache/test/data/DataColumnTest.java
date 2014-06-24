package data;

import junit.framework.TestCase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import data.DataColumn.DataColumnType;

/**
 * 
 * @author Anderson Mao, 2014-06-23
 */
public class DataColumnTest extends TestCase{
	@Override
	public void setUp(){
		Logger.getRootLogger().setLevel(Level.DEBUG);
	}
	
	@Test
	public void testConvertDataColumnValueDate(){
		Long value1 = (Long)DataColumn.convertDataColumnValue(DataColumnType.DATE, "2014-06-22");
		Long value2 = (Long)DataColumn.convertDataColumnValue(DataColumnType.DATE, "2014-06-23");
		assertTrue(value1 < value2);
	}
	
	@Test
	public void testParseDataColumnValueDate(){
		Long value1 = (Long)DataColumn.parseDataColumnValue(DataColumnType.DATE, "2014-06-23 10:11:12");
		Long value2 = (Long)DataColumn.parseDataColumnValue(DataColumnType.DATE, "2014-06-23 10:11:13");
		assertTrue(value1 < value2);
	}
}
