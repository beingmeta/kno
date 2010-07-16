(in-module 'xhtml/download)

(use-module '{fdweb xhtml reflection})

(module-export! 'xhtml/download)

(define boundary "<frontier>")

(define (xhtml/download . specs)
  (when (> (length specs) 2)
    (begin
      (cgiset! 'content-type
	       (stringout "multipart/mixed; boundary=" boundary))
      (httpheader "Mime-Version: 1.0")
      (xhtml "\nThis response contains " (/ (length specs) 2) " files\n")
      (do ((scan specs
		 (if (and (not (string? (car scan)))
			  (test (car scan) 'content))
		     (cdr scan)
		     (cddr scan))))
	  ((null? scan)
	   (xhtml "\r\n--" boundary "--\r\n"))
	(xhtml "\r\n--" boundary "\r\n")
	(if (and (not (string? (car scan))) (test (car scan) 'content))
	    (write-attachment (car scan) (get (car scan) 'content))
	    (write-attachment (car scan) (cadr scan))))))
  (when (<= (length specs) 2)
    (let* ((spec (car specs))
	   (info (if (string? spec) #[] spec))
	   (name (if (string? spec) spec (get spec 'filename)))
	   (content (if (= (length specs) 2) (cadr specs)
			(get info 'content)))
	   (type (try (get info 'content-type) (guess-content-type name content)
		      "text; charset=utf-8")))
      (cgiset! 'content-type type)
      (httpheader (stringout "Content-Disposition: attachment; filename=" name))
      (write-content content))))

(define (write-attachment spec content)
  (let* ((info (if (string? spec) #[] spec))
	 (name (if (string? spec) spec (get spec 'filename)))
	 (type (try (get info 'content-type) (guess-content-type name content)
		    "text; charset=utf-8")))
    (xhtml "Content-Type: " type "\r\n")
    (xhtml "Content-Disposition: attachment; filename=" name "\r\n\r\n")
    (write-content content)))
(define (write-content content)
  (if (string? content) (xhtml content)
      (if (packet? content) ()
	  (if (and (table? content) (test content '%xmltag))
	      (xmleval content)
	      (if (applicable? content) (content)
		  (xhtml content))))))

(define (guess-content-type name content)
  (if (exists has-suffix (downcase name) {".html" ".htm" ".xhtml"})
      "text/html; charset=utf-8;"
      (if (has-suffix (downcase name) ".manifest")
	  "text/cache-manifest; charset=utf-8"
	  (if (has-suffix (downcase name) ".png") "image/png"
	      (if (has-suffix (downcase name) ".gif") "image/gif"
		  (if (exists has-suffix (downcase name) {".jpg" ".jpeg"})
		      "image/jpeg"
		      "text; charset=utf-8"))))))

