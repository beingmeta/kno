;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'texttools)

(define s1
  "This is a \u0073imple \u{0073}entence, with a \146ew \U00000073pecial characters: \\ \? \' \&amp; \&middot;")

(applytest #t equal? s1
	   "This is a simple sentence, with a few special characters: \\ ? ' & ·")

(applytest #\middot elt "·" 0)
(applytest #\newline elt "\nxx" 0)
(applytest #\space elt " xx" 0)
(applytest #\tab elt "	xx" 0)
(applytest #\backspace elt "xx" 0)
(applytest #\bell elt "\axx" 0)
(applytest #\bell elt "xx" 0)
(applytest #\ding elt "xx" 0)
(applytest #\attention elt "xx" 0)
(applytest #\page elt "\fxx" 0)
(applytest #\vtab elt "\vxx" 0)
(applytest #\verticaltab elt "\vxx" 0)
(applytest #\return elt "\rxx" 0)
(applytest #\slash elt "/xx" 0)
(applytest #\backslash elt "\\xx" 0)
(applytest #\openparen elt "(xx" 0)
(applytest #\closeparen elt ")xx" 0)
(applytest #\openbrace elt "{xx" 0)
(applytest #\closebrace elt "}xx" 0)
(applytest #\openbracket elt "[xx" 0)
(applytest #\closebracket elt "]xx" 0)

(define string-data
  (file->dtype (get-component "./data/i18n/UTF-8.dtype")))

(applytest string-data
	   (filestring  (get-component "./data/i18n/UTF-8.text") "utf-8"))

;; Remove it if it exists
(when (file-exists? (get-component "utf8-temp.text"))
  (remove-file (get-component "utf8-temp.text")))

(display string-data
	 (open-output-file (get-component "utf8-temp.text") "utf8"))
(dtype->file string-data (get-component "utf8-temp.dtype"))

(applytest string-data
	   file->dtype (get-component "utf8-temp.dtype"))

(message "Anti-Warning: Expect a lot of UTF-8 error warnings")
(define stress-string
  (filestring (get-component "./data/i18n/stress-UTF-8.text")
	      "UTF8"))
(define stress-lines
  (subseq (segment stress-string "\n") 60 -1))

(applytest 79 length (elts stress-lines))

(message "Anti-Warning: You can ignore the flock of UTF-8 warnings above,  \
          It was the UTF-8 stress test.")

(test-finished "i18n")
