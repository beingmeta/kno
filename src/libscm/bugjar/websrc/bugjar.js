function tbodyToggle(evt){
    evt=evt||event;
    var target=evt.target||evt.relatedTarget;
    var scan=target; while (scan) {
	if (((scan.tagName==="A")&&(scan.href))||(scan.onclick))
	    return false;
	else if (scan.tagName==="TBODY") break;
	else if ((scan.className)&&
		 (typeof scan.className==="string")&&
		 (scan.className.search(/\bexpands\b/)>=0))
	    break;
	else scan=scan.parentNode;}
    if (!(scan)) return;
    if (scan.nodeType!==1) return;
    if (scan.classList)
	scan.classList.toggle("expanded");
    else if (!(scan.className)) scan.className="expanded";
    else if (scan.className.search(/\bexpanded\b/)>=0)
	scan.className=scan.className.replace(/\bexpanded\b/,"").trim();
    else scan.className=scan.className+" expanded";}
