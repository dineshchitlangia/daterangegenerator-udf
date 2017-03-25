package com.dc.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * UDF to generate all dates given [startDate, endDate, dateformat]
 * dateFormat is expected to be as per Joda specs.
 */
public final class DateRangeGenerator extends GenericUDF
{

	Converter[] converters = null;
	StringObjectInspector inputOI = null;
	List<Object> dates = new ArrayList<>();

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
		if(objectInspectors.length != 3) {
			throw new UDFArgumentException("All params are required: startDate, count and dateFormat");
		}

		if (!(objectInspectors[0] instanceof StringObjectInspector)
				|| !(objectInspectors[1] instanceof StringObjectInspector)
				|| !(objectInspectors[2] instanceof StringObjectInspector)) {
			throw new UDFArgumentException("All input should be of type STRING");
		}

		this.inputOI = (StringObjectInspector) objectInspectors[0];
		this.converters = new Converter[objectInspectors.length];
		converters[0] = ObjectInspectorConverters.getConverter(inputOI, PrimitiveObjectInspectorFactory.writableStringObjectInspector);

		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		String start = inputOI.getPrimitiveJavaObject(deferredObjects[0].get());
		String end = inputOI.getPrimitiveJavaObject(deferredObjects[1].get());
		String dateFormat = inputOI.getPrimitiveJavaObject(deferredObjects[2].get());

		if (start == null || end == null || dateFormat == null) {
			return null;
		}

		DateTimeFormatter formatter = DateTimeFormat.forPattern(dateFormat);
		DateTime startDate = null;
		DateTime endDate = null;
		try {
			startDate = formatter.parseDateTime(start);
			endDate = formatter.parseDateTime(end);
		} catch (IllegalArgumentException e) {
			System.err.println("ERROR: DateRangeToDateArray - can't parse startDate or count: " + startDate + " " + endDate);
			startDate = null;
			endDate = null;
		}

		if ( startDate == null || endDate == null ) {
			return null;
		}
		
		return getDatesArrayForRange(startDate, endDate, formatter);
	}

	private List<Object> getDatesArrayForRange(DateTime start,
			DateTime end, DateTimeFormatter formatter) {
		
		dates.clear();
		while (start.isBefore(end) || start.isEqual(end)) {
			Text dateAsText = new Text(start.toString(formatter));
			dates.add(dateAsText);
			start = start.plusDays(1);
		}
		return dates;
	}
	
	@Override
	public String getDisplayString(String[] strings) {
		return "Generate all dates given [startDate, endDate, dateformat]";
	}
}
