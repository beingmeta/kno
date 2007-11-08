(in-module 'xhtml/clickit)
(use-module 'fdweb)
(use-module 'xhtml)

(define (commandclick command label title)
  (anchor* (stringout "javascript:_fdb_clickit_command(':" command "');")
	   ((class "clickit")
	    (title title)
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);")	    )
	   label))
(define (actionclick action label title)
  (anchor* (stringout "javascript:_fdb_clickit_action('" action "');")
	   ((class "clickit")
	    (title title)
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);"))
	   label))
(define (acclick action command label title)
  (anchor* (stringout "javascript:_fdb_clickit_action_command('" action "',':" command "');")
	   ((class "clickit")
	    (title title)
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);"))
	   label))
(define (urlclick url label title)
  (anchor* url
	   ((class "clickit")
	    (title title)
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);"))
	   label))
(define (scriptclick script label title (alturl #f))
  (anchor* (or alturl "javascript:")
	   ((class "clickit")
	    (title title)
	    (onclick (stringout script "; return false;"))
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);"))
	   label))
(define (xurlclick url label title)
  (anchor* url
	   ((class "clickit") (title title) (target "_blank")
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);"))
	   label))
(module-export!
 '{commandclick
   actionclick
   urlclick
   xurlclick
   acclick
   scriptclick})

;;; Tab buttons

(define (tabbutton text image content
		   (selectvar 'livetab) (defaultselect #f)
		   (title #f))
  (if (and selectvar (cgitest selectvar))
      (when (cgitest selectvar content)
	(input id (stringout selectvar "_INPUT") 
	       type "HIDDEN" name (symbol->string selectvar)
	       value content))
      (when defaultselect
	(when selectvar
	  (cgiset! selectvar content)
	  (input id (stringout selectvar "_INPUT") 
		 type "HIDDEN" name (symbol->string selectvar)
		 value content))))
  (span ((class (if (if selectvar (cgitest selectvar content)
			defaultselect)
		    "selected_tab"
		    "tab"))
	 (selectvar (if selectvar (symbol->string selectvar)))
	 (onmouseover "_fdb_tab_mouseover(event);")
	 (onmouseout "_fdb_tab_mouseout(event);")
	 (onclick "return _fdb_tab_click(event);")
	 (title (if title title))
	 (contentid content))
    (if image
	(image SRC image ALT text)
	text)))

(module-export! 'tabbutton)

;;; Hide/show buttons

(define (hideshow contentid hidetext showtext (var #f) (title #f))
  (span ((class "hideshow")
	 (onclick "_fdb_hideshow_toggle(event);")
	 (title (or title "click to hide or reveal additional content"))
	 (content contentid))
    "["
    (span ((class "whenHidden")) hidetext)
    (span ((class "whenVisible")) showtext)
    "]"))

(module-export! 'hideshow)

;;; Vistoggle

(define vistoggle
  (macro expr
    `(xmlblock SPAN
	 ((class "vistoggle_hidden")
	  (onclick "_fdb_vistoggle(event);")
	  (content ,(get-arg expr 1)))
       ,@(subseq expr 2))))

(module-export! 'vistoggle)

;;; Hot checkboxes

(define (hotcheck var val (text #f) (title #f))
  (span ((class "hotcheck") (title (if title title))
	 (onclick "_fdb_hotcheck_click(event);")
	 (style (if (cgitest var val) "font-weight: bold;")))
    (input TYPE "radio" NAME (symbol->string var) VALUE val
	   ("CHECKED" (cgitest var val)))
    (or text val)))

(module-export! 'hotcheck)

;;; Text input boxes which contain a prompt when blurred and empty

(define autoprompt
  (macro expr
    `(input TYPE "TEXT" class "autoprompt"
	    ,@(cdr expr)
	    onfocus "return _fdb_autoprompt_focus(event);"
	    onblur "return _fdb_autoprompt_blur(event);"
	    onload "return _fdb_autoprompt_onload(event);")))

(module-export! 'autoprompt)


;;; Font changing

(define (font-sizers)
  (span ((class "font_sizer")
	 (onclick "decrease_font(event);")
	 (title "click to decrease font size"))
    "-")
  (span ((class "font_sizer")
	 (onclick "increase_font(event);")
	 (title "click to increase font size"))
    "+"))

(define (fontsizers) (font-sizers))

(module-export! '{font-sizers fontsizers})


