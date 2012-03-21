package trendulo.web.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import trendulo.web.model.Trends;
import trendulo.web.model.Trends.Trend;
import trendulo.web.query.QueryService;

@Controller
public class TrendsController {
	
	@Autowired
	private QueryService queryService;
	
	private Logger log = Logger.getLogger( TrendsController.class );
	
	@RequestMapping(value = "/trends/{dateGranularity}/{dateString}", method = RequestMethod.GET)
	public @ResponseBody Trends getTrends( @PathVariable String dateGranularity, @PathVariable String dateString ) {
		
		log.debug( "DATE GRANULARITY:" + dateGranularity + " TIMESTAMP:" + dateString );
		Trends trends = new Trends();
		trends.setDateString("March 01, 2012");
		List<Trend> trendList = new ArrayList<Trend>();
		SortedMap<Integer,String> trendMap = queryService.getTrends(dateString, 25);
		for ( Entry<Integer,String> entry : trendMap.entrySet() ) {
			trendList.add( 0, trends.new Trend( entry.getValue(), entry.getKey() ));
		}
		trends.setTrends(trendList);
		return trends;
	}

}
