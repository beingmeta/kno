;; Indentation information

(require 'cmuscheme)

;; We do this because we don't want console windows to have infinite undo
(make-variable-buffer-local 'undo-limit)
;; The name of the FramerD scheme module for a particular buffer
(make-variable-buffer-local 'fdconsole-module)

(defvar *framerd-keywords*
  '("\\<do-choices-mt\\>" "\\<do-seq-mt\\>"
    "\\<do-choices\\>" "\\<for-choices\\>" "\\<filter-choices\\>"
    "\\<doseq\\>" "\\<dolist\\>" "\\<dotimes\\>" "\\<forseq>\\"
    "\\<lambda\\>" "\\<ambda\\>" "\\<slambda\\>"
    "\\<try\\>" "\\<tryif\\>"
    "\\<while\\>" "\\<until\\>" "\\<onerror>\\"
    "\\<find-frames\\>" "\\<pick\\>" "\\<reject\\>"
    "\\<div\\>" "\\<p\\>" "\\<p*\\>" "\\<form\\>"
    "\\<try-choices>\\" "\\<tryseq>\\" "\\<extdb/proc>\\"))

;; This gets #[ and #( to do block indents
(defun scheme-indent-function (indent-point state)
  (let ((normal-indent (current-column)))
    (goto-char (1+ (elt state 1)))
    (parse-partial-sexp (point) calculate-lisp-indent-last-sexp 0 t)
    (if (and (elt state 2)
	     (or (not (looking-at "\\sw\\|\\s_"))
		 (save-excursion
		   (goto-char (elt state 1))
		   (looking-at "{"))
		 (save-excursion
		   (goto-char (- (elt state 1) 1))
		   (or (looking-at "#(") (looking-at "#\\[") (looking-at "#{"))))
             ;; (not (looking-at "\\sw\\|\\s_"))
	     )
        ;; car of form doesn't seem to be a symbol
        (progn
          (if (not (> (save-excursion (forward-line 1) (point))
                      calculate-lisp-indent-last-sexp))
              (progn (goto-char calculate-lisp-indent-last-sexp)
                     (beginning-of-line)
                     (parse-partial-sexp (point)
					 calculate-lisp-indent-last-sexp 0 t)))
          ;; Indent under the list or under the first sexp on the same
          ;; line as calculate-lisp-indent-last-sexp.  Note that first
          ;; thing on that line has to be complete sexp since we are
          ;; inside the innermost containing sexp.
          (backward-prefix-chars)
          (current-column))
      (let ((function (buffer-substring (point)
					(progn (forward-sexp 1) (point))))
	    method)
	(setq method (or (get (intern-soft function) 'scheme-indent-function)
			 (get (intern-soft function) 'scheme-indent-hook)))
	(cond ((or (eq method 'defun)
		   (and (null method)
			(> (length function) 3)
			(string-match "\\`def" function)))
	       (lisp-indent-defform state indent-point))
	      ((integerp method)
	       (lisp-indent-specform method state
				     indent-point normal-indent))
	      (method
		(funcall method state indent-point normal-indent)))))))

(defun block-indenter (state indent-point normal)
  (save-excursion
    (goto-char (elt state 1))
    (+ (current-column) 2)))

;;; FRAMERD stuff
(put 'when 'scheme-indent-function 1)
(put 'unless 'scheme-indent-function 1)
(put 'tryif 'scheme-indent-function 1)

(put 'ambda 'scheme-indent-function 1)
(put 'sambda 'scheme-indent-function 1)
(put 'slambda 'scheme-indent-function 1)

(put 'begin 'scheme-indent-function 'block-indenter)

(put 'dolist 'scheme-indent-function 1)
(put 'do-pool 'scheme-indent-function 1)
(put 'dotimes 'scheme-indent-function 1)
(put 'doseq 'scheme-indent-function 1)
(put 'dopool 'scheme-indent-function 1)
(put 'do-choices 'scheme-indent-function 1)
(put 'do-choices-mt 'scheme-indent-function 1)
(put 'do-seq-mt 'scheme-indent-function 1)
(put 'do-subsets 'scheme-indent-function 1)
(put 'for-choices 'scheme-indent-function 1)
(put 'forseq 'scheme-indent-function 1)
(put 'filter-choices 'scheme-indent-function 1)
(put 'try-choices 'scheme-indent-function 1)
(put 'tryseq 'scheme-indent-function 1)
(put 'while 'scheme-indent-function 1)
(put 'until 'scheme-indent-function 1)
(put 'with-lock 'scheme-indent-function 1)
(put 'onerror 'scheme-indent-function 1)
(put 'prog1 'scheme-indent-function 1)

(put 'index-frame 'scheme-indent-function 2)

(put 'hashtable-increment! 'scheme-indent-function 2)
(put 'hashtable-increment-existing! 'scheme-indent-function 2)
(put 'table-increment! 'scheme-indent-function 2)
(put 'table-increment-existing! 'scheme-indent-function 2)
(put 'hashtable-multiply! 'scheme-indent-function 2)
(put 'hashtable-multiply-existing! 'scheme-indent-function 2)
(put 'table-multiply! 'scheme-indent-function 2)
(put 'table-multiply-existing! 'scheme-indent-function 2)

(put 'unwind-protect 'scheme-indent-function 1)
(put 'on-errors 'scheme-indent-function 1)

(put 'printout-to 'scheme-indent-function 1)
(put 'hmac-sha1 'scheme-indent-function 1)
(put 'printout 'scheme-indent-function 'block-indenter)
(put 'lineout 'scheme-indent-function 'block-indenter)
(put 'fileout 'scheme-indent-function 1)
(put 'writeout 'scheme-indent-function 1)
(put 'writeout/type 'scheme-indent-function 2)
(put 'gp/writeout 'scheme-indent-function 2)
(put 'gp/writeout! 'scheme-indent-function 2)
(put 'gp/write! 'scheme-indent-function 2)
(put 'gp/save! 'scheme-indent-function 1)
(put 's3/op 'scheme-indent-function 3)
(put 's3/write! 'scheme-indent-function 1)
(put 'ses/call 'scheme-indent-function 1)
(put 'stringout 'scheme-indent-function 'block-indenter)
(put 'glom 'scheme-indent-function 'block-indenter)
(put 'logif 'scheme-indent-function 1)
(put 'logger 'scheme-indent-function 1)

(put 'sig/make 'scheme-indent-function 1)
(put 'sig/check 'scheme-indent-function 2)

(put 'req/set! 'scheme-indent-function 1)
(put 'req/add! 'scheme-indent-function 1)

(put 'with-output 'scheme-indent-function 1)
(put 'with-output-to-string 'scheme-indent-function 'block-indenter)
(put 'string-subst* 'scheme-indent-function 1)

;;; XML/HTML generation
(put 'xmlout 'scheme-indent-function 'block-indenter)
(put 'xmlblock 'scheme-indent-function 2)
(put 'xmlblockn 'scheme-indent-function 2)
(put 'xmlelt 'scheme-indent-function 'block-indenter)
(put 'scripturl 'scheme-indent-function 2)
(put 'fdscripturl 'scheme-indent-function 2)

(put 'soapenvelope 'scheme-indent-function 2)

(put 'xhtml 'scheme-indent-function 'block-indenter)
(put 'span 'scheme-indent-function 1)
(put 'div 'scheme-indent-function 1)
(put 'table* 'scheme-indent-function 1)
(put 'p 'scheme-indent-function 'block-indenter)
(put 'h1 'scheme-indent-function 'block-indenter)
(put 'h2 'scheme-indent-function 'block-indenter)
(put 'h3 'scheme-indent-function 'block-indenter)
(put 'h4 'scheme-indent-function 'block-indenter)
(put 'anchor 'scheme-indent-function 1)
(put 'anchor* 'scheme-indent-function 2)
(put 'p* 'scheme-indent-function 1)
(put 'h1* 'scheme-indent-function 1)
(put 'h2* 'scheme-indent-function 1)
(put 'tr* 'scheme-indent-function 1)
(put 'th* 'scheme-indent-function 1)
(put 'td* 'scheme-indent-function 1)
(put 'ul 'scheme-indent-function 1)
(put 'li 'scheme-indent-function 1)
(put 'ul* 'scheme-indent-function 1)
(put 'li* 'scheme-indent-function 1)
(put 'vistoggle 'scheme-indent-function 1)
(put 'form 'scheme-indent-function 1)

(put 'find-frames 'scheme-indent-function 1)
(put 'frame-create 'scheme-indent-function 1)

(put '%watch 'scheme-indent-function 1)
(put 'debug%watch 'scheme-indent-function 1)
(put 'info%watch 'scheme-indent-function 1)
(put 'notice%watch 'scheme-indent-function 1)
(put 'saveoutput 'scheme-indent-function 1)

(put 'extdb/proc 'scheme-indent-function 1)

;;;; Evaluating expressions in modules

(defvar fdconsole-module)
(defconst in-module-regexp
  "(in-module +'\\(\\(\\w\\|[/$.-_]\\)+\\)")

(defun fdconsole-get-module-name ()
  "Returns the module name specified in the current buffer"
  (if (and (boundp 'fdconsole-module) fdconsole-module) fdconsole-module
    (save-excursion
      (goto-char (point-min))
      (let* ((pos (search-forward-regexp in-module-regexp (point-max) t))
	     (name
	      (if pos
		  (buffer-substring (match-beginning 1) (match-end 1))
		"")))
;	(if pos
;	    (message "Search found module name %s at %d" name pos)
;	  (message "Search failed to find module name"))
	(if pos (setq fdconsole-module name))
	name))))
(defun fdconsole-process () (scheme-proc))

(defun fdconsole-send-region (start end)
  (let ((module (fdconsole-get-module-name))
	(process (fdconsole-process)))
    (if (not (equal module ""))
	(message "Sending %d characters into the %s module"
		 (- end start) module))
    (if (not (equal module ""))
	(comint-send-string
	 process (format "(within-module '%s\n" module)))
    (comint-send-region process start end)
    (if (not (equal module ""))
	(comint-send-string process ")\n")
      (comint-send-string process "\n"))))

(defun fdconsole-send-definition ()
  "Send the current definition to the inferior Scheme process."
  (interactive)
  (save-excursion
   (end-of-defun)
   (let ((end (point)))
     (beginning-of-defun)
     (fdconsole-send-region (point) end))))

(defun scheme-send-last-sexp ()
  "Send the previous sexp to the inferior Scheme process."
  (interactive)
  (fdconsole-send-region (save-excursion (backward-sexp) (point)) (point)))

(defun fdconsole-sender ()
  (interactive)
  (if mark-active (fdconsole-send-region (region-beginning) (region-end))
    (fdconsole-send-definition)))

(defun split-command-line (string)
  (let ((where (string-match "[ \t]" string)))
    (cond ((null where) (list string))
	  ((not (= where 0))
	   (cons (substring string 0 where)
		 (split-command-line (substring string (+ 1 where)
						(length string)))))
	  (t (let ((pos (string-match "[^ \t]" string)))
	       (if (null pos)
		   nil
		 (split-command-line (substring string pos
						(length string)))))))))

;;; Running an fdconsole

(defvar fdconsole-program "fdconsole")

(autoload 'comint-check-proc "comint")

(defun fdconsole (cmd)
  "Run an inferior FramerD scheme process, input and output via buffer *fdconsole*.
With an arguments, prompts for a command and arguments to use.
If there is a process already running in `*fdconsole*', switch to that buffer.
Runs the hooks `inferior-scheme-mode-hook' \(after the `comint-mode-hook' is
run). \(Type \\[describe-mode] in the process buffer for a list of commands.)"
  (interactive
   (list (if current-prefix-arg
	     (read-string "Run fdconsole: " fdconsole-program)
	   fdconsole-program)))
  (if (not (comint-check-proc "*fdconsole*"))
      (let ((cmdlist (split-command-line cmd)))
	(set-buffer (apply 'make-comint "fdconsole" (car cmdlist)
			   nil (cdr cmdlist)))
	(inferior-scheme-mode)))
  (setq scheme-program-name cmd)
  (setq scheme-buffer "*fdconsole*")
  (pop-to-buffer "*fdconsole*"))

;;; Defining a mode hook to define fdconsole-sender

(defun fdconsole-scheme-mode-hook ()
  (interactive)
  (local-set-key "\e\C-m" 'fdconsole-sender)
  (setq undo-limit 32)
  (font-lock-add-keywords 'scheme-mode *framerd-keywords*))
(add-hook 'scheme-mode-hook 'fdconsole-scheme-mode-hook)

