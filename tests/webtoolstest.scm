(load-component "common.scm")

(use-module 'webtools)

(define test-input-1
  "<p a='b' c ='d' e= 'f' g = 'h' i=j k =l m= n o = p>some text</p>")

(define test-result-1
  `(,(sort-slotmap
      `#[%XMLTAG  P %CONTENT  ("some text") %ATTRIBS 
	 {#("a" #f "b") #("c" #f "d") #("e" #f "f") #("g" #f "h") #("i" #f "j")
	  #("k" #f "l") #("m" #f "n") #("o" #f "p")} %QNAME  P %ATTRIBIDS 
	 {A C E G I K M O}
	 A  "b" C  "d" E  "f" G  "h" I  "j" K  "l" M  "n" O  "p"])))
(applytester test-result-1 xmlparse test-input-1)

(define test-input-2 
  "<p title=\"From &ldquo;greek&rdquo;\">From Α to Ω</p>")
(define test-result-2
  '(#[%XMLTAG  P %CONTENT  ("From Α to Ω")
      %ATTRIBS  #("title" #f "From “greek”")
      %QNAME  P %ATTRIBIDS  TITLE 
      TITLE "From “greek”"]))
(applytester test-result-2 xmlparse test-input-2)

(define test-input-3
  "<p>Two plus three is <?=(+ 2 3)?> </p>")
(define test-result-3
  '(#[%XMLTAG P %CONTENT 
      ("Two plus three is " (BEGIN (+ 2 3)) " ") %QNAME P]))
(applytester test-result-3 fdxml/parse test-input-3)
(evaltest "<p>Two plus three is 5 </p>"
	  (xml->string test-result-3))

(define test-input-4
  "<p title=\"$(glom 'alpha 2 #f 'omega)\">Two plus three is <?=(+ 2 3)?>.</p>")
(define test-result-4
  `(,(sort-slotmap
      #[%XMLTAG  P 
	%CONTENT  ("Two plus three is " (BEGIN (+ 2 3)) ".")
	%ATTRIBS #("title" #f (%XMLEVAL GLOM 'ALPHA 2 #f 'OMEGA))
	%QNAME  P %ATTRIBIDS  TITLE 
	TITLE (%XMLEVAL GLOM 'ALPHA 2 #f 'OMEGA)])))
(applytester test-result-4 fdxml/parse test-input-4)
(evaltest "<p title=\"ALPHA2OMEGA\">Two plus three is 5.</p>"
	  (xml->string test-result-4))

