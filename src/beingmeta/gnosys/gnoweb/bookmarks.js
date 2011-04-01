/* -*- Mode: C; -*- */

function bookmark_tag_click(evt)
{
  hint_copy_click(evt);
  document.forms[0].submit();
}

/*

// This is the number of milliseconds to wait until reverting to the
//  default category display.  This is to avoid a bouncy display when
//  going from category to category.
var hysteresis=1000;
var topcat=false;
var topcat_elt=false;
var default_display_interval;

function default_category_display()
{
  if (default_display_interval) {
    clearInterval(default_display_interval);
    default_display_interval=false;}
  if (topcat) {
    var bookmarks=fdbByID('bookmarks');
    var nodes=bookmarks.childNodes;
    var scelt=fdbOid2Elt(topcat,'SC');
    hide_subcategories();
    if (scelt) scelt.style.display='block';
    topcat_elt.style.background='lightgray';
    var i=0; while (i<nodes.length) {
      var node=nodes[i++];
      if ((node.nodeType==1) && (node.className=='bookmark')) {
	var tags=fdbGet(node,'tags');
	if ((tags) && (tags.indexOf(topcat)>=0)) node.style.display='block';
	else node.style.display='none';}}}
  else {
    var bookmarks=fdbByID('bookmarks');
    var nodes=bookmarks.childNodes;
    var showall=fdbByID('SHOWALL');
    hide_subcategories();
    if (showall) showall.style.background='lightgray';
    var i=0; while (i<nodes.length) {
      var node=nodes[i++];
      if ((node.nodeType==1) && (node.className=='bookmark'))
	node.style.display='block';}}
}

function hide_subcategories()
{
  var subcategories_elt=fdbByID('subcats');
  if (subcategories_elt) {
    var subcategories=subcategories_elt.childNodes;
    var i=0; while (i<subcategories.length) {
      var node=subcategories[i++];
      if ((node.nodeType==1) && (node.className=='subcategories'))
	node.style.display='none';}}
}

function category_mouseover(evt)
{
  var evelt=evt.target;
  var elt=fdbFindClass(evelt,'metakey');
  if ((elt) && (elt.getAttribute)) {
    if (default_display_interval) {
      clearInterval(default_display_interval);
      default_display_interval=false;}
    var oid=fdbGet(elt,'oid');
    var oidplus=oid+';';
    var bookmarks=fdbByID('bookmarks');
    var scelt=fdbOid2Elt(oid,'SC');
    var nodes=bookmarks.childNodes;
    if (topcat_elt) topcat_elt.style.background='inherit';
    else {
      var showall=fdbByID('SHOWALL');
      if (showall) showall.style.background='inherit';}
    elt.style.background='lightgray';
    if (scelt) {
      hide_subcategories(); scelt.style.display='block';}
    var i=0; while (i<nodes.length) {
      var node=nodes[i++]; 
      if ((node.nodeType==1) && (node.className=='bookmark')) {
	var tags=fdbGet(node,'tags');
	// alert('bookmark '+i+' has tags: '+tags);
	if ((tags) && (tags.indexOf(oidplus)>=0)) {
	  node.style.display='block';}
	else node.style.display='none';}}}
}
function category_mouseout(evt)
{
  var evelt=evt.target;
  if (evelt.className=='metakey') {
    if (evelt != topcat_elt) 
      evelt.style.background='inherit';
    default_display_interval=
      setInterval("default_category_display();",hysteresis);}
}

function category_mouseclick(event)
{
  var elt=fdbFindClass(event.target,'metakey');
  if (elt) {
    var oid=fdbGet(elt,'oid');
    // alert('topcat_elt='+elt);
    topcat=oid; topcat_elt=elt;}
}

function showall_mouseover(event)
{
  var evtelt=event.target;
  // alert('showall mouseover');
  if (default_display_interval) {
    clearInterval(default_display_interval);
    default_display_interval=false;}
  var bookmarks=fdbByID('bookmarks');
  var nodes=bookmarks.childNodes;
  if (topcat_elt) topcat_elt.style.background='inherit';
  evtelt.style.background='lightgray';
  var i=0; while (i<nodes.length) {
    var node=nodes[i++];
    if ((node.nodeType==1) && (node.className=='bookmark'))
      node.style.display='block';}
}
function showall_mouseout(event)
{
  if (evelt != topcat_elt) {
    evelt.style.background='inherit';
    topcat_elt.style.background='lightgray';}
  default_display_interval=
    setInterval("default_category_display();",hysteresis);
}
function showall_mouseclick(evt)
{
  topcat=false; topcat_elt=evt.target;
}

function subcategory_mouseover(evt)
{
  var evelt=evt.target;
  var elt=fdbFindClass(evelt,'metakey');
  // if (elt==null) alert('no metakey from elt '+evelt+' class='+evelt.className);
  if ((elt) && (elt.getAttribute)) {
    if (default_display_interval) {
      clearInterval(default_display_interval);
      default_display_interval=false;}
    default_category_display();
    var oid=fdbGet(elt,'oid');
    var superoid=elt.fdbGet(parentNode,'oid');
    var oidplus=oid+';';
    var superoidplus=superoid+';';
    var bookmarks=fdbByID('bookmarks');
    var nodes=bookmarks.childNodes;
    // alert('selectively showing for '+oid+' and '+superoid);
    if (topcat_elt) topcat_elt.style.background='inherit';
    else {
      var showall=fdbByID('SHOWALL');
      if (showall) showall.style.background='inherit';}
    elt.style.background='lightgray';
    var i=0; while (i<nodes.length) {
      var node=nodes[i++]; 
      if ((node.nodeType==1) && (node.className=='bookmark')) {
	var tags=fdbGet(node,'tags');
	// alert('bookmark '+i+' has tags: '+tags);
	if ((tags) && (tags.indexOf(oidplus)>=0) &&
	    (tags.indexOf(superoidplus)>=0)) {
	  node.style.display='block';}
	else node.style.display='none';}}}
  return true;
}

function bookmark_showtags(evt)
{
  var evtelt=evt.target;
  var bookmark=fdbFindClass(evtelt,'bookmark');
  var child=findChild(bookmark,'tags');
  if (child)
    if (child.style.display=='none')
      child.style.display='block';
    else child.style.display='none';
}

function bookmark_showtags(evt)
{
  var evtelt=evt.target;
  var bookmark=fdbFindClass(evtelt,'bookmark');
  var child=findChild(bookmark,'tags');
  var excerpts=findChild(bookmark,'excerpts');
  if (excerpts) excerpts.style.display='none';
  if (child)
    if (child.style.display=='none')
      child.style.display='block';
    else child.style.display='none';
}

function bookmark_showexcerpts(evt)
{
  var evtelt=evt.target;
  var bookmark=fdbFindClass(evtelt,'bookmark');
  var tags=findChild(bookmark,'tags');
  if (tags) tags.style.display='none';
  var child=findChild(bookmark,'excerpts');
  if (child)
    if (child.style.display=='none')
      child.style.display='block';
    else child.style.display='none';
}

function search_category_mouseover(evt)
{
  metakey_mouseover(evt);
  category_mouseover(evt);
}

function search_category_mouseout(evt)
{
  metakey_mouseout(evt);
  category_mouseout(evt);
}

*/
