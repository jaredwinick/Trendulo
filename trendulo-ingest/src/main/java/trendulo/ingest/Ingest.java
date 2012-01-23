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

package trendulo.ingest;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Ingest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		ApplicationContext context = new ClassPathXmlApplicationContext( "applicationContext.xml" );
		final TemporalNGramSource nGramSource = (TemporalNGramSource) context.getBean( "nGramSource" );
		Thread nGramSourceThread = new Thread( nGramSource );
		nGramSourceThread.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() { 
		    	nGramSource.shutdown();
		     }
		 });
	}

}
