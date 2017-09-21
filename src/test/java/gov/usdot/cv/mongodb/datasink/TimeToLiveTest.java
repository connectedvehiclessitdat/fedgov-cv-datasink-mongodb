package gov.usdot.cv.mongodb.datasink;

import static org.junit.Assert.*;
import gov.usdot.cv.common.util.UnitTestHelper;
import gov.usdot.cv.mongodb.datasink.model.TimeToLive;

import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Level;
import org.junit.Test;

public class TimeToLiveTest {
	
	static Level loggerLvl = Level.INFO; 
	
	static
	{
		UnitTestHelper.initLog4j(loggerLvl);
		Properties testProperties = System.getProperties();
		if (testProperties.getProperty("RTWS_CONFIG_DIR") == null) {
			testProperties.setProperty("RTWS_CONFIG_DIR", testProperties.getProperty("basedir", ".") + "/src/test/resources");
			System.setProperties(testProperties);
		}
	}
	
	@Test
	public void testTimeToLive() {
		Date now = new Date(System.currentTimeMillis());
		System.out.println("Current Time: " +  now);
		
		Calendar checker = null;
		Date expiration = null;
		
		TimeToLive minute = TimeToLive.fromCode(0);
		expiration = minute.getExpiration();
		System.out.println("Minute Later: " +  expiration);
		
		checker = Calendar.getInstance();
		checker.add(Calendar.SECOND, 45);
		assertTrue(expiration.after(checker.getTime()));
		
		// -----
		
		TimeToLive hh = TimeToLive.fromCode(1);
		expiration = hh.getExpiration();
		System.out.println("Half Hour Later: " +  expiration);
		
		checker = Calendar.getInstance();
		checker.add(Calendar.MINUTE, 25);
		assertTrue(expiration.after(checker.getTime()));
		
		// -----
		
		TimeToLive day = TimeToLive.fromCode(2);
		expiration = day.getExpiration();
		System.out.println("Day Later: " +  expiration);
		
		checker = Calendar.getInstance();
		checker.add(Calendar.HOUR, 23);
		assertTrue(expiration.after(checker.getTime()));
		
		// -----
		
		TimeToLive week = TimeToLive.fromCode(3);
		expiration = week.getExpiration();
		System.out.println("Week Later: " +  expiration);
		
		checker = Calendar.getInstance();
		checker.add(Calendar.DATE, 6);
		assertTrue(expiration.after(checker.getTime()));
		
		// -----
		
		TimeToLive month = TimeToLive.fromCode(4);
		expiration = month.getExpiration();
		System.out.println("Month Later: " +  expiration);
		
		checker = Calendar.getInstance();
		checker.add(Calendar.DATE, 25);
		assertTrue(expiration.after(checker.getTime()));
		
		// -----
		
		TimeToLive year = TimeToLive.fromCode(5);
		expiration = year.getExpiration();
		System.out.println("Year Later: " +  expiration);
		
		checker = Calendar.getInstance();
		checker.add(Calendar.MONTH, 11);
		assertTrue(expiration.after(checker.getTime()));
		
		// -----
		
		TimeToLive def = TimeToLive.fromCode(15);
		assertNull("Code 15 is invalid should return null.", def);
	}
	
}