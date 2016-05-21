package com.echat.storm.analysis;

import java.util.Date;
import java.util.Calendar;
import java.util.HashMap;
import org.apache.commons.lang.time.DateFormatUtils;
import com.google.gson.Gson;

public class AppTpsCounter {
	public final String app;
	public final Date dateStart;
	public Date periodStart;

	public long totalUnordered;
	public long totalInfo;
	public long totalWarn;
	public long totalError;
	public long totalFatal;
	public long totalEvent;

	public int tpsInfo;
	public int tpsWarn;
	public int tpsError;
	public int tpsEvent;

	public HashMap<String,Integer> eventCounter = new HashMap<String,Integer>();
	public transient String lastReport;

	public AppTpsCounter(String app,Date date) {
		this.app = app;
		this.dateStart = date;

		totalUnordered = 0;
		totalInfo = 0;
		totalWarn = 0;
		totalError = 0;
		totalFatal = 0;
		totalEvent = 0;

		nextPeriod(date);
	}

	public boolean info(Date date) {
		boolean timeout = updatePeriod(date);
		totalInfo++;
		tpsInfo++;
		return timeout;
	}

	public boolean warn(Date date) {
		boolean timeout = updatePeriod(date);
		totalWarn++;
		tpsWarn++;
		return timeout;
	}

	public boolean error(Date date) {
		boolean timeout = updatePeriod(date);
		totalError++;
		tpsError++;
		return timeout;
	}

	public boolean fatal(Date date) {
		boolean timeout = updatePeriod(date);
		totalFatal++;
		return timeout;
	}

	public void event(Date date,String ev) {
		Integer count = eventCounter.get(ev);
		if( count == null ) {
			count = 0;
		}
		count++;
		eventCounter.put(ev,count);

		totalEvent++;
		tpsEvent++;
	}

	private Date cutoffMs(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	private boolean updatePeriod(Date date) {
		if( date.before(periodStart) ) {
			totalUnordered += 1;
			return false;
		}
		if( (date.getTime() - periodStart.getTime()) >= 1000 ) { // 1 second
			generateReport();
			nextPeriod(date);
			return true;
		} else {
			return false;
		}
	}

	private void generateReport() {
		/*
		JSONObject json = new JSONObject();
		json.put("app",app);
		json.put("start",DateFormatUtils.format(dateStart,"yyyy-MM-dd HH:mm:ss"));
		json.put("period",DateFormatUtils.format(periodStart,"yyyy-MM-dd HH:mm:ss"));

		json.put("totalUnordered",totalUnordered);
		json.put("totalInfo",totalInfo);
		json.put("totalWarn",totalWarn);
		json.put("totalError",totalError);
		json.put("totalFatal",totalFatal);

		json.put("tspInfo",tpsInfo);
		json.put("tpsWarn",tpsWarn);
		json.put("tpsError",tpsError);
		json.put("tpsEvent",tpsEvent);

		json.put("events",new JSONObject().putAll(eventCounter));

		lastReport = json.toString();
		*/
		Gson gson = new Gson();
		lastReport = gson.toJson(this);
	}

	private void nextPeriod(Date date) {
		periodStart = cutoffMs(date);
		tpsInfo = 0;
		tpsWarn = 0;
		tpsError = 0;
		tpsEvent = 0;
		eventCounter.clear();
	}
}

