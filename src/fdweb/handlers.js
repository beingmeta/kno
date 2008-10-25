/* Various utility handlers */

var msgalert=false;

function fdbmsg(string)
{
  if ((console) && (console.log))
    console.log(string);
  else if (msgalert)
    alert(string);
}

/* INPUT SHOWHELP */

function block_eltp(elt)
{
  var name=helptext.tagName;
  return ((name=='DIV') || (name=='P') || (name=='LI') || (name=='UL'));
}

function fdb_showhelp_onfocus(evt)
{
  var target=evt.target;
  if ((target.tagName == 'INPUT') && (target.hasAttribute('HELPTEXT'))) {
    var helptext=document.getElementById(target.getAttribute('HELPTEXT'));
    if (helptext) {
      if (block_eltp(helptext))
	helptext.style.display='block';
      else helptext.style.display='inline';}}
}

function fdb_hidehelp_onblur(evt)
{
  var target=evt.target;
  if ((target.tagName == 'INPUT') && (target.hasAttribute('HELPTEXT'))) {
    var helptext=document.getElementById(target.getAttribute('HELPTEXT'));
    if (helptext) helptext.style.display='none';}
}

  /* SHOWHIDE */

function fdb_showhide_onclick(evt)
{
  var target=evt.target;
  while (target)
    if ((target.hasAttribute('HIDEONCLICK')) || (target.hasAttribute('SHOWONCLICK'))) {
      var tohide=target.getAttribute('HIDEONCLICK');
      var toshow=target.getAttribute('SHOWONCLICK');
      if (tohide) tohide=tohide.split(',');
      if (toshow) toshow=toshow.split(',');
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
      return;}
    else target=target.parentNode;
}

/* Autoprompt */

function fdb_autoprompt_onfocus(evt)
{
  var elt=evt.target;
  if ((elt) && (elt.hasAttribute('isempty'))) {
    elt.value='';
    elt.removeAttribute('isempty');}
  fdb_showhelp_onfocus(evt);
}

function fdb_autoprompt_onblur(evt)
{
  var elt=evt.target;
  if (elt.value=='') {
    elt.setAttribute('isempty','yes');
    elt.value=elt.getAttribute('prompt');}
  else elt.removeAttribute('isempty');
  fdb_hidehelp_onblur(evt);
}

function fdb_autoprompt_setup()
{
  var elements=document.getElementsByTagName('INPUT');
  var i=0; if (elements) while (i<elements.length) {
    var elt=elements[i++];
    if ((elt.type=='text') &&
	((elt.className=='autoprompt') ||
	 (elt.hasAttribute('prompt')))) {
      var prompt=elt.getAttribute('prompt');
      if (prompt==null)
	if (elt.className=='autoprompt')
	  prompt=elt.title;
	else continue;
      // fdbmsg('Considering '+elt+' class='+elt.className+' value='+elt.value);
      if ((elt.value=='') || (elt.value==prompt)) {
	// fdbmsg('Marking empty');
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
    while (elt)
      if (elt.hasAttribute("contentid")) break;
      else elt=elt.parentNode;
    if (elt==null) return;
    var select_var=elt.getAttribute('selectvar');
    var content_id=elt.getAttribute('contentid');
    var content=document.getElementById(content_id);
    var select_elt=document.getElementById(select_var+'_INPUT');
    var parent=elt.parentNode;
    var sibs=parent.childNodes;
    if (content==null) {
      fdbmsg("No content for "+content_id);
      return;}
    // evt.preventDefault(); evt.cancelBubble=true;
    // This lets forms pass tab information along
    if (select_elt) select_elt.value=content_id;
    var i=0; while (i<sibs.length) {
      var node=sibs[i++];
      if ((node.nodeType==1) && (node.className=='tab')) {
	var cid=node.getAttribute('contentid');
	var cdoc=document.getElementById(cid);
	if (node==elt) {}
	else if (node.hasAttribute('shown')) {
	  node.removeAttribute('shown');
	  if (cdoc) cdoc.removeAttribute('shown');}}}
    elt.setAttribute('shown','yes');
    content.setAttribute('shown','yes');
    return false;}
}

/* Setup */

function fdb_setup()
{
  fdbmsg("fdb_setup running")
  fdb_autoprompt_setup();
  fdbmsg("fdb_setup run")

}
