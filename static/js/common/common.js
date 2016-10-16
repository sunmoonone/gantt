/**
 * jquery 扩展
 */
(function($) {
	var escapeable = /["\\\x00-\x1f\x7f-\x9f]/g, meta = {
		'\b' : '\\b',
		'\t' : '\\t',
		'\n' : '\\n',
		'\f' : '\\f',
		'\r' : '\\r',
		'"' : '\\"',
		'\\' : '\\\\'
	};

	/**
	 * jQuery.toJSON Converts the given argument into a JSON respresentation.
	 * 
	 * @param o
	 *            {Mixed} The json-serializble *thing* to be converted
	 * 
	 * If an object has a toJSON prototype, that will be used to get the
	 * representation. Non-integer/string keys are skipped in the object, as are
	 * keys that point to a function.
	 * 
	 */
	$.toJSON = typeof JSON === 'object' && JSON.stringify
			? JSON.stringify
			: function(o) {

				if (o === null) {
					return 'null';
				}

				var type = typeof o;

				if (type === 'undefined') {
					return undefined;
				}
				if (type === 'number' || type === 'boolean') {
					return '' + o;
				}
				if (type === 'string') {
					return $.quoteString(o);
				}
				if (type === 'object') {
					if (typeof o.toJSON === 'function') {
						return $.toJSON(o.toJSON());
					}
					if (o.constructor === Date) {
						var month = o.getUTCMonth() + 1, day = o.getUTCDate(), year = o
								.getUTCFullYear(), hours = o.getUTCHours(), minutes = o
								.getUTCMinutes(), seconds = o.getUTCSeconds(), milli = o
								.getUTCMilliseconds();

						if (month < 10) {
							month = '0' + month;
						}
						if (day < 10) {
							day = '0' + day;
						}
						if (hours < 10) {
							hours = '0' + hours;
						}
						if (minutes < 10) {
							minutes = '0' + minutes;
						}
						if (seconds < 10) {
							seconds = '0' + seconds;
						}
						if (milli < 100) {
							milli = '0' + milli;
						}
						if (milli < 10) {
							milli = '0' + milli;
						}
						return '"' + year + '-' + month + '-' + day + 'T'
								+ hours + ':' + minutes + ':' + seconds + '.'
								+ milli + 'Z"';
					}
					if (o.constructor === Array) {
						var ret = [];
						for (var i = 0; i < o.length; i++) {
							ret.push($.toJSON(o[i]) || 'null');
						}
						return '[' + ret.join(',') + ']';
					}
					var name, val, pairs = [];
					for (var k in o) {
						type = typeof k;
						if (type === 'number') {
							name = '"' + k + '"';
						} else if (type === 'string') {
							name = $.quoteString(k);
						} else {
							// Keys must be numerical or string. Skip others
							continue;
						}
						type = typeof o[k];

						if (type === 'function' || type === 'undefined') {
							// Invalid values like these return undefined
							// from toJSON, however those object members
							// shouldn't be included in the JSON string at all.
							continue;
						}
						val = $.toJSON(o[k]);
						pairs.push(name + ':' + val);
					}
					return '{' + pairs.join(',') + '}';
				}
			};

	/**
	 * jQuery.evalJSON Evaluates a given piece of json source.
	 * 
	 * @param src
	 *            {String}
	 */
	$.evalJSON = typeof JSON === 'object' && JSON.parse
			? JSON.parse
			: function(src) {
				return eval('(' + src + ')');
			};

	/**
	 * jQuery.secureEvalJSON Evals JSON in a way that is *more* secure.
	 * 
	 * @param src
	 *            {String}
	 */
	$.secureEvalJSON = typeof JSON === 'object' && JSON.parse
			? JSON.parse
			: function(src) {

				var filtered = src
						.replace(/\\["\\\/bfnrtu]/g, '@')
						.replace(
								/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g,
								']').replace(/(?:^|:|,)(?:\s*\[)+/g, '');

				if (/^[\],:{}\s]*$/.test(filtered)) {
					return eval('(' + src + ')');
				} else {
					throw new SyntaxError('Error parsing JSON, source is not valid.');
				}
			};

	/**
	 * jQuery.quoteString Returns a string-repr of a string, escaping quotes
	 * intelligently. Mostly a support function for toJSON. Examples: >>>
	 * jQuery.quoteString('apple') "apple"
	 * 
	 * >>> jQuery.quoteString('"Where are we going?", she asked.') "\"Where are
	 * we going?\", she asked."
	 */
	$.quoteString = function(string) {
		if (string.match(escapeable)) {
			return '"' + string.replace(escapeable, function(a) {
						var c = meta[a];
						if (typeof c === 'string') {
							return c;
						}
						c = a.charCodeAt();
						return '\\u00' + Math.floor(c / 16).toString(16)
								+ (c % 16).toString(16);
					}) + '"';
		}
		return '"' + string + '"';
	};
	$.jsonEncode = $.json_encode = $.toJSON;
	$.jsonDecode = $.json_decode = $.evalJSON;
})(jQuery);


(function($){
	
    function setOffset(el, newOffset){
        var $el = $(el);
 
        // set position to relative if it's static
        if ($el.css('position') == 'static') {
           return;
        } 
 
        if ($.isNumeric(newOffset.left)){
            $el.css('left', newOffset.left + 'px');
        }
        if ($.isNumeric(newOffset.top)){
            $el.css('top', newOffset.top + 'px');
        }
    }
    /**
	 * Set or get the specific left and top css style of the matched elements
	 * 
	 * @param {Object}
	 *            newOffset { left:{number} top:{number} }
	 */
    $.fn.cssOffset=function(newOffset){
    	 return !newOffset ? this.offset() : this.each(function(){
         	setOffset(this, newOffset);
         });
    };
    
    /**
     * 解决 调用 val 赋值不触发 change 事件
     */
    var valFun=$.fn.val;
    $.fn.val=function(value){
    	if ( !arguments.length ) {
    		return valFun.call(this);
    	}
    	var ret = valFun.call(this,value);
    	$(this).trigger('custom.change');
    	return ret;

//      infinite loop
//    	if(!changefiring){
//            changefiring=true;
//            this.change();
//    	}
//    	changefiring=false;
    };
	
    $.isObject = function(obj) {
		if(obj===null||obj===undefined)return false;
		return typeof obj == 'object';
	};
	
	var _origIsString=$.isString;

	$.isString=function(val){
		if(_origIsString){
			return _origIsString(val);
		}
		return $.type(val)=="string";
	}
	
	$.isset=function(theVar){
		return theVar!==undefined;
	}
	 /**
		 * 
		 * @param {mixed}
		 *            val
		 */
    $.isEmpty=function(val){
    	if(val==null || val==undefined ||val=='' ||val==0)return true;
    	if($.isArray(val)){
    		return val.length==0;
    	}
    	if($.isObject(val)){
    		return $.isEmptyObject(val);
    	}
    	return false;
    }
    
	$.toLower=function(val){
		if(val==null || val==undefined)return val;
		return val.toLowerCase();
	};
	
	$.toUpper=function(val){
		if(val==null || val==undefined)return val;
		return val.toUpperCase();
	}

    $.fn.serializeObject = function() {
        var o = {};
        $.each(this.serializeArray(), function() {
            if (o[this.name]) {
                if (!o[this.name].push) {
                    o[this.name] = [ o[this.name] ];
                }
                o[this.name].push(this.value || '');
            } else {
                o[this.name] = this.value || '';
            }
        });
        return o;
    };
	
})(jQuery);
