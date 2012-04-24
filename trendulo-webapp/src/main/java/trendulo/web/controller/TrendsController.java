// =================================================================================================
// Copyright 2012 Jared Winick
// -------------------------------------------------------------------------------------------------
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this work except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file, or at:
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =================================================================================================


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
