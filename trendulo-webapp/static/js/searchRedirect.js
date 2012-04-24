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


$(document).ready(function(){
	
	$('#searchForm').submit( function() {
		// build the url that we will redirect to. default to 7 day search
		var searchText = $('#searchInputText').val().toLowerCase();
		var searchTextEscaped = escape(searchText);
		var url = "/?search=" + searchTextEscaped + "&days=7";
		window.location = url;
		return false;
	});
	
});