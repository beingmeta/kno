/* Various utilities for manipulating the dom */

function $(eltarg)
{
  if (typeof eltarg == 'string')
    return document.getElementById(eltarg);
  else return eltarg;
}

function fdbAddElements(elt,args,i)
{
  while (i<args.length) {
    var arg=args[i++];
    if (typeof arg == 'string')
      elt.appendChild(document.createTextNode(arg));
    else elt.appendChild(arg);}
  return elt;
}

function fdbAddAttributes(elt,attribs)
{
  if (attribs) {
    for (key in attribs) {
      if (key=='title')
	elt.title=attribs[key];
      else if (key=='name')
	elt.name=attribs[key];
      else if (key=='id')
	elt.id=attribs[key];
      else if (key=='value')
	elt.value=attribs[key];
      else elt.setAttribute(key,attribs[key]);}
    return elt;}
  else return elt;
}

function fdbNewElement(tag,class)
{
  var elt=document.createElement(tag);
  elt.className=class;
  fdbAddElements(elt,arguments,2);
  return elt;
}

function fdbNewElementW(tag,class,attribs)
{
  var elt=document.createElement(tag);
  elt.className=class;
  fdbAddAttributes(elt,attribs);
  fdbAddElements(elt,arguments,3);
  return elt;
}

function fdbSpan(class)
{
  var elt=document.createElement('span');
  elt.className=class;
  fdbAddElements(elt,arguments,1);
  return elt;
}

function fdbSpanW(class,attribs)
{
  var elt=document.createElement('span');
  elt.className=class;
  fdbAddAttributes(elt,attribs);
  fdbAddElements(elt,arguments,2);
  return elt;
}

function fdbDiv(class)
{
  var elt=document.createElement('div');
  elt.className=class;
  fdbAddElements(elt,arguments,1);
  return elt;
}

function fdbDivW(class,attribs)
{
  var elt=document.createElement('div');
  elt.className=class;
  fdbAddAttributes(elt,attribs);
  fdbAddElements(elt,arguments,2);
  return elt;
}

function fdbInput(type,name,value,class)
{
  var elt=document.createElement('input');
  elt.type=type; elt.name=name; elt.value=value;
  if (class) elt.className=class;
  return elt;
}

function fdbCheckbox(name,value,checked)
{
  var elt=document.createElement('input');
  elt.type='checkbox'; elt.name=name; elt.value=value;
  if (checked) elt.checked=true;
  else elt.checked=false;
  return elt;
}

