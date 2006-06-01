(use-module 'texttools)

(define string-data
  (file->dtype (get-component "../data/i18n/UTF-8.dtype")))

(applytest string-data
	   filestring  (get-component "../data/i18n/UTF-8.text") "utf-8")

(display string-data
	 (open-output-file (get-component "utf8-temp.text") "utf8"))
(dtype->file string-data (get-component "utf8-temp.dtype"))

(applytest string-data
	   file->dtype (get-component "utf8-temp.dtype"))

(message "Anti-Warning: Expect a lot of UTF-8 error warnings")
(define stress-string
  (filestring (get-component "../data/i18n/stress-UTF-8.text")
	      "UTF8"))
(define stress-lines
  (subseq (segment stress-string "\n") 60 -1))

(applytest 79 length (elts stress-lines))

(message "i18n tests finished")
