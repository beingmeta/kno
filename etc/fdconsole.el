;; Indentation information

(defvar *framerd-keywords*
  '("\\<do-choices-mt\\>" "\\<do-seq-mt\\>"
    "\\<do-choices\\>" "\\<for-choices\\>" "\\<filter-choices\\>"
    "\\<doseq\\>" "\\<dolist\\>" "\\<dotimes\\>"
    "\\<lambda\\>" "\\<ambda\\>" "\\<slambda\\>"
    "\\<while\\>" "\\<until\\>" 
    "\\<find-frames\\>" "\\<pick\\>" "\\<reject\\>"
    "\\<div\\>" "\\<p\\>" "\\<p*\\>" "\\<form\\>"
    "\\<try-choices>\\" "\\<tryseq>\\"))

;;; FRAMERD stuff
(put 'when 'scheme-indent-function 1)
(put 'unless 'scheme-indent-function 1)

(put 'ambda 'scheme-indent-function 1)
(put 'sambda 'scheme-indent-function 1)
(put 'slambda 'scheme-indent-function 1)

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
(put 'filter-choices 'scheme-indent-function 1)
(put 'try-choices 'scheme-indent-function 1)
(put 'tryseq 'scheme-indent-function 1)
(put 'while 'scheme-indent-function 1)
(put 'until 'scheme-indent-function 1)

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
(put 'printout 'scheme-indent-function 0)
(put 'lineout 'scheme-indent-function 0)
(put 'fileout 'scheme-indent-function 0)
(put 'stringout 'scheme-indent-function 0)
(put 'logif 'scheme-indent-function 1)
(put 'logger 'scheme-indent-function 1)

(put 'with-output 'scheme-indent-function 1)
(put 'with-output-to-string 'scheme-indent-function 0)

;;; XML/HTML generation
(put 'xmlout 'scheme-indent-function 0)
(put 'xmlblock 'scheme-indent-function 2)
(put 'xmlblockn 'scheme-indent-function 2)
(put 'xmlelt 'scheme-indent-function 0)

(put 'soapenvelope 'scheme-indent-function 2)

(put 'xhtml 'scheme-indent-function 0)
(put 'span 'scheme-indent-function 1)
(put 'div 'scheme-indent-function 1)
(put 'table* 'scheme-indent-function 1)
(put 'p 'scheme-indent-function 0)
(put 'h1 'scheme-indent-function 0)
(put 'h2 'scheme-indent-function 0)
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
(put 'li* 'scheme-indent-function 2)
(put 'form 'scheme-indent-function 1)

(put 'find-frames 'scheme-indent-function 1)
(put 'frame-create 'scheme-indent-function 1)

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
	(if pos
	    (progn (make-variable-buffer-local 'fdconsole-module)
		   (setq fdconsole-module name)))
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

;;; Running an fdconsole

(defvar fdconsole-program "fdconsole")

(autoload 'scheme-args-to-list "cmuscheme")
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
      (let ((cmdlist (scheme-args-to-list cmd)))
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
  (font-lock-add-keywords 'scheme-mode *framerd-keywords*))
(add-hook 'scheme-mode-hook 'fdconsole-scheme-mode-hook)

