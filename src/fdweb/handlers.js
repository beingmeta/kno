/* INPUT SHOWHELP */

function block_eltp(elt)
{
  var name=elt.tagName;
  return ((name==='DIV') || (name==='P') || (name==='LI') || (name==='UL'));
}

function fdb_showhelp_onfocus(evt)
{
  var target=evt.target;
  if ((target.nodeType==1) &&
      (target.tagName === 'INPUT') &&
      (target.hasAttribute('HELPTEXT'))) {
    var helptext=document.getElementById(target.getAttribute('HELPTEXT'));
    if (helptext) {
      if (block_eltp(helptext))
	helptext.style.display='block';
      else helptext.style.display='inline';}}
}

function fdb_hidehelp_onblur(evt)
{
  var target=evt.target;
  if ((target.nodeType==1) &&
      (target.tagName === 'INPUT') &&
      (target.hasAttribute('HELPTEXT'))) {
    var helptext=document.getElementById(target.getAttribute('HELPTEXT'));
    if (helptext) helptext.style.display='none';}
}

/* SHOWHIDE */

function fdb_showhide_onclick(evt)
{
  var target=evt.target;
  // fdbLog('target='+target);
  while (target.parentNode)
    if ((target.hasAttribute!=null) &&
	(target.hasAttribute('CLICKTOHIDE')) ||
	(target.hasAttribute('CLICKTOSHOW')) ||
	(target.hasAttribute('CLICKTOTOGGLE'))) {
      var tohide=target.getAttribute('CLICKTOHIDE');
      var toshow=target.getAttribute('CLICKTOSHOW');
      var toflip=target.getAttribute('CLICKTOTOGGLE');
      if (tohide) tohide=tohide.split(',');
      if (toshow) toshow=toshow.split(',');
      if (toflip) toflip=toflip.split(',');
      if (tohide) {
	var i=0; while (i<tohide.length) {
	  var elt=document.getElementById(tohide[i]); 
	  if (elt) elt.style.display='none';
	  i++;}}
      if (toshow) {
	var i=0; while (i<toshow.length) {
	  var elt=document.getElementById(toshow[i]); 
	  if (elt)
	    if (block_eltp(elt)) elt.style.display='block';
	    else elt.style.display='block';
	  i++;}}
      if (toflip) {
	var i=0; while (i<toflip.length) {
	  var elt=document.getElementById(toflip[i]);
	  if (elt) {
	    var display=elt.style.display;
	    if ((display===null) || (display===''))
	      display=window.getComputedStyle(elt,null).display;
	    if (display==='none')
	      if (block_eltp(elt)) elt.style.display='block';
	      else elt.style.display='inline';
	    else elt.style.display='none';}
	  i++;}}
      return;}
    else target=target.parentNode;
}

/* SETCLEAR */

function fdb_setclear_onclick(evt)
{
  var target=evt.target;
  while (target.parentNode)
    if ((target.hasAttribute!=null) &&
	(target.hasAttribute('SETONCLICK'))) {
      var toset=target.getAttribute('SETONCLICK');
      var attrib=target.getAttribute('SETATTRIB');
      var val=target.getAttribute('ATTRIBVAL');
      if (val===null) val='set';
      if ((toset) && (toset!='')) toset=toset.split(',');
      else return;
      if (toset) {
	var i=0; while (i<toset.length) {
	  var elt=document.getElementById(toset[i++]);
	  if (elt===null) return;
	  if (elt.hasAttribute(attrib))
	    elt.removeAttribute(attrib);
	  else elt.setAttribute(attrib,val);}}
      return;}
    else target=target.parentNode;
}

/* Autoprompt */

function fdb_autoprompt_onfocus(evt)
{
  var elt=evt.target;
  if ((elt) && (elt.hasAttribute) && (elt.hasAttribute('isempty'))) {
    elt.value='';
    elt.removeAttribute('isempty');}
  fdb_showhelp_onfocus(evt);
}

function fdb_autoprompt_onblur(evt)
{
  var elt=evt.target;
  if (elt.value==='') {
    elt.setAttribute('isempty','yes');
    var prompt=elt.getAttribute('prompt');
    if ((prompt===null) && (elt.className==='autoprompt'))
      prompt=elt.title;
    elt.value=prompt;}
  else elt.removeAttribute('isempty');
  fdb_hidehelp_onblur(evt);
}

function fdb_autoprompt_setup()
{
  var elements=document.getElementsByTagName('INPUT');
  var i=0; if (elements) while (i<elements.length) {
    var elt=elements[i++];
    if ((elt.type==='text') &&
	((elt.className==='autoprompt') ||
	 (elt.hasAttribute('prompt')))) {
      var prompt=elt.getAttribute('prompt');
      if (prompt===null)
	if (elt.className==='autoprompt')
	  prompt=elt.title;
	else continue;
      fdbLog('Considering '+elt+' class='+elt.className+' value='+elt.value);
      if ((elt.value=='') || (elt.value==prompt)) {
	fdbLog('Marking empty');
	elt.value=prompt;
	elt.setAttribute('isempty','yes');}}}
}

// Removes autoprompt text from empty fields
function fdb_autoprompt_cleanup()
{
  var elements=document.getElementsByTagName('INPUT');
  var i=0; if (elements) while (i<elements.length) {
    var elt=elements[i++];
    if (elt.hasAttribute('isempty'))
      elt.value="";}
}

/* Tabs */

function fdb_tab_onclick(evt)
{
  var elt=evt.target;
  if (elt) {
    while (elt.parentNode)
      if ((elt.hasAttribute) && (elt.hasAttribute("contentid"))) break;
      else elt=elt.parentNode;
    if (elt===null) return;
    var select_var=elt.getAttribute('selectvar');
    var content_id=elt.getAttribute('contentid');
    var content=document.getElementById(content_id);
    var select_elt=document.getElementById(select_var+'_INPUT');
    var parent=elt.parentNode;
    var sibs=parent.childNodes;
    if (content===null) {
      fdbLog("No content for "+content_id);
      return;}
    // evt.preventDefault(); evt.cancelBubble=true;
    // This lets forms pass tab information along
    if (select_elt) select_elt.value=content_id;
    var i=0; while (i<sibs.length) {
      var node=sibs[i++];
      if ((node.nodeType===1) && (node.className==='tab')) {
	var cid=node.getAttribute('contentid');
	var cdoc=document.getElementById(cid);
	if (node===elt) {}
	else if (node.hasAttribute('shown')) {
	  node.removeAttribute('shown');
	  if (cdoc) cdoc.removeAttribute('shown');}}}
    elt.setAttribute('shown','yes');
    content.setAttribute('shown','yes');
    return false;}
}

/* Adding search engines */

var browser_coverage="Mozilla/Firefox/Netscape 6";

function addBrowserSearchPlugin(spec,name,cat)
{
  if ((typeof window.sidebar === "object") &&
      (typeof window.sidebar.addSearchEngine === "function")) 
    window.sidebar.addSearchEngine (spec+'.src',spec+".png",name,cat);
  else alert(browser_coverage+" is needed to install a search plugin");
}

/* FLEXPAND click */

/* Enables region which expand and contract on a click. */

function fdb_flexpand_onclick(event)
{
  var target=event.target; var functional=false;
  while (target.parentNode)
    if (!(target.hasAttribute!=null)) target=target.parentNode;
    else if (target.hasAttribute('expanded')) break;
    else if (target.tagName==='A')
      return;
    else if ((target.tagName==='SELECT') ||
	     (target.tagName==='INPUT') ||
	     (target.className=='checkspan') ||
	     (target.onclick!=null)) {
      functional=true;
      target=target.parentNode;}
    else target=target.parentNode;
  if (target) {
    if (target.getAttribute('expanded')==="yes")
      if (functional) {}
      else {
	target.setAttribute("expanded","no");
	target.style.maxHeight=null;}
    else {
      target.setAttribute("expanded","yes");
      target.style.maxHeight='inherit';}}
}

/* Closing the window */

function fdb_close_window(interval)
{
  if (interval)
    window.setTimeout(_fdb_close_window,interval*1000);
  else window.close();
}

function _fdb_close_window(event)
{
  window.close();
}

/* checkspan handling */

function fdb_checkspan_onclick(event)
{
  var target=event.target;
  while (target.parentNode) {
    if (target.nodeType!=1) target=target.parentNode;
    else if (target.className==='checkspan') break;
    else if ((target.tagName==='A') || (target.tagName==='INPUT'))
      return;
    else target=target.parentNode;}
  // if (target) fdbLog('Found checkspan '+target);
  if (target) {
    var children=target.childNodes;
    var i=0; while (i<children.length) {
      var child=children[i++];
      if ((child.nodeType===1) &&
	  (child.tagName==='INPUT') &&
	  ((child.type==='radio') ||
	   (child.type==='checkbox'))) {
	var checked=child.checked;
	if (typeof(checked) === null) {
	  child.checked=false;
	  target.removeAttribute('ischecked');}
	else if (checked) {
	  child.checked=false;
	  target.removeAttribute('ischecked');}
	else {
	  child.checked=true;
	  target.setAttribute('ischecked','yes');}}}}
}

function fdb_checkspan_setup(checkspan)
{
  if (checkspan===null) {
    var elements=document.getElementsByClassName('checkspan');
    var i=0; if (elements) while (i<elements.length) {
      var checkspan=elements[i++];
      fdb_checkspan_setup(checkspan);}}
  else {
    var children=checkspan.childNodes;
    var j=0; while (j<children.length) {
      var child=children[j++];
      if ((child.nodeType===1) &&
	  (child.tagName==='INPUT') &&
	  ((child.type==='radio') ||
	   (child.type==='checkbox'))) {
	var checked=child.checked;
	if (checked === null) {
	  child.checked=false;
	  checkspan.removeAttribute('ischecked');}
	else if (checked) {
	  checkspan.setAttribute('ischecked','yes');}}}}
}

/* Cheshire handling */

var cheshire_elt=null;
var cheshire_steps=false;
var cheshire_countdown=false;
var cheshire_timer=false;
var cheshire_finish=null;

function _fdb_cheshire_handler(event)
{
  if ((cheshire_steps) &&
      (cheshire_countdown<=0)) {
    console.log('closing window');
    clearInterval(cheshire_timer);
    if (cheshire_finish)
      cheshire_finish();
    else window.close();}
  else if ((cheshire_steps) &&
	   (cheshire_countdown)) {
    cheshire_countdown=cheshire_countdown-1;
    var ratio=(cheshire_countdown/cheshire_steps)*0.8;
    // console.log('opacity='+ratio);
    cheshire_elt.style.opacity=ratio;}
  else {}
}

function fdb_start_cheshire(eltid,interval,steps)
{
  if (typeof(interval) === 'undefined') interval=30;
  if (typeof(steps) === 'undefined') steps=interval*2;
  if (typeof(eltid) === 'undefined')
    cheshire_elt=document.body;
  else if (typeof(eltid) === 'string')
    cheshire_elt=document.getElementById(eltid);
  else cheshire_elt=eltid;
  if (cheshire_elt===null) cheshire_elt=document.body;
  console.log('Starting cheshire over '+interval+' for '+steps);
  cheshire_elt.style.opacity=.80;
  cheshire_steps=steps;
  cheshire_countdown=steps;
  cheshire_timer=setInterval(_fdb_cheshire_handler,(1000*interval)/steps);
}

function fdb_cheshire_onclick(event)
{
  if (cheshire_elt) {
    cheshire_elt.style.opacity='inherit';
    clearInterval(cheshire_timer);
    cheshire_steps=false;
    cheshire_countdown=false;}
}

/* Text checking */

/* This handles automatic handling of embedded content, for
 * example tags or other markup. It creates an interval timer
 * to check for changes in the value of an input field.
 * eltid is the textfield to monitor
 * textfn is the function to parse its value
 * changefn is the function to call on the parse result when it changes
 * interval is how often to check for changes
 */
function fdb_textract(eltid,textfn,changefn,interval)
{
  /* Default the value for interval and normalize it to
     milliseconds if it looks like its actually seconds. */
  if (interval==null) interval=4000;
  else if (interval<200) interval=interval*1000;
  var elt=document.getElementById(eltid);
  if (elt==null) return;
  var text=elt.value, parsed=textfn(text);
  if (parsed) changefn(parsed);
  // console.log('Init text='+text);
  // console.log('Init parsed='+parsed);
  // console.log('Init interval='+interval);
  var loop_fcn=function(event) {
    if (elt.value!=text) {
      var new_parsed=textfn(elt.value);
      // console.log('New text='+elt.value);
      //console.log('New parsed='+new_parsed);
      text=elt.value;
      if (new_parsed!=parsed) {
	parsed=new_parsed;
	changefn(parsed);}}};
  window.setInterval(loop_fcn,interval);
}

/* Font size adjustment */

function fdb_adjust_font_size(elt)
{
  var target_width=elt.getAttribute('targetwidth');
  var target_height=elt.getAttribute('targetheight');
  var actual_width=elt.clientWidth;
  var actual_height=elt.clientHeight;
  if (((target_width==null) || (actual_width<target_width)) &&
      ((target_height==null) || (actual_height<target_height)))
    return;
  var x_ratio=((target_width==null) ? (1.0) : (target_width/actual_width));
  var y_ratio=((target_height==null) ? (1.0) : (target_height/actual_height));
  var do_ratio=((x_ratio<y_ratio) ? (x_ratio) : (y_ratio));
  elt.style.fontSize=(do_ratio*100.0)+"%";
  // The code below, if it worked, would shrink and then expand the element
  // However, it doesn't work because the actual width doesn't get updated
  // automatically
  /*
  var step=(1.0-do_ratio)/5;
  var new_ratio=do_ratio;
  elt.style.fontSize=(do_ratio*100.0)+"%";
  while (((target_width==null) || (actual_width<target_width)) &&
	 ((target_height==null) || (actual_height<target_height))) {
    do_ratio=new_ratio;
    new_ratio=do_ratio+step;
    elt.style.fontSize=(do_ratio*100.0)+"%";
    actual_width=elt.clientWidth;
    actual_height=elt.clientHeight;}
  */
}

function fdb_adjust_font_sizes()
{
  var elts=document.getElementsByClassName('autosize');
  if (elts) {
    var i=0; while (i<elts.length)
      fdb_adjust_font_size(elts[i++]);}
}

function fdb_adjust_font_sizes()
{
  var elts=document.getElementsByClassName('autosize');
  if (elts) {
    var i=0; while (i<elts.length)
      fdb_adjust_font_size(elts[i++]);}
}

/* Handling CSS based reduction */

function fdb_mark_reduced(elt)
{
  if (elt) {
    var target_width=elt.getAttribute('targetwidth');
    var target_height=elt.getAttribute('targetheight');
    var actual_width=elt.clientWidth;
    var actual_height=elt.clientHeight;
    if (((target_width===null) || (actual_width<target_width)) &&
	((target_height===null) || (actual_height<target_height)))
      return;
    else {
      var classinfo=elt.className;
      if (classinfo) 
	if (classinfo.search(/\breduced\b/)>=0) {}
	else elt.className=classinfo+' reduced';
      else elt.className='reduced';
      // fdbLog('Reducing '+elt+' to class '+elt.className);
    }}
  else {
    var elts=document.getElementsByClassName('autoreduce');
    var i=0; while (i<elts.length) fdb_mark_reduced(elts[i++]);}
}


/* Setup */

function fdb_setup()
{
  fdbLog("fdb_setup running");
  fdb_autoprompt_setup();
  fdb_checkspan_setup(null);
  fdb_adjust_font_sizes();
  fdb_mark_reduced();
  fdbLog("fdb_setup run");
}
