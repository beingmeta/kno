function toggleExpand(evt)
{
    if (!(evt)) evt = event || window.evt;
    var target = (evt) && (evt.target);
    while (target) {
	if ( (target.className) && (typeof target.className == "string") ) {
	    var classname = target.className;
	    if (classname.search(/\bexpands\b/)) {
		if (classname.search(/\bexpanded\b/)>0) {
		    var newclass = classname.replace(/\bexpanded\b/,"");
		    target.className = newclass;}
		else {
		    var newclass = classname+" expanded";
		    target.className = newclass;}
		break;}}
	target = target.parentNode;}
}

