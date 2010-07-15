(in-module 'xhtml/download)

(use-module '{fdweb xhtml})

(define boundary "<frontier">)

(define (xhtml/download . specs)
  (if (> (length specs) 2)
      (begin (cgiset! 'content-type "multipart/mixed; boundary=\"" frontier "\"")
	     (httpheader "Mime-Version: 1.0")
	     (output-download (car specs) )
	     (xhtml "\nThis response contains " (/ (length specs) 2) " files\n")
	     (do ((scan specs
			(if (and (not (string? (car scan))) (test (car scan) 'content))
			    (cdr scan)
			    (cddr scan))))
		 ((null? scan)
		  (xhtml "\r\n--<frontier>--\r\n"))
	       (xhtml "\r\n--<frontier>\r\n")
	       (if (and (not (string? (car scan))) (test (car scan) 'content))
		   (write-attachment (car scan) (get (car scan) 'content))
		   (write-attachment (car scan) (cadr scan)))))
      (if (= (length specs) 2) (write-attachment (car scan) (cadr scan))
	  (write-attachment (car scan) (get (car scan) 'content)))))

(define (write-content spec content)
  (let* ((info (if (string? spec) #[] spec))
	 (name (if (string? spec) spec (get spec 'filename)))
	 (type (try (get info 'content-type) (guess-content-type name content)
		    "text; charset=utf-8")))
    (xhtml "\r\n--<frontier>\r\n")
    (xhtml "Content-Type: " type "\r\n")
    (xhtml "Content-Disposition: attachment; filename=" name "\r\n\r\n")
    (if (string? content) (xhtml content)
	(if (packet? content) ()
	    (if (and (table? content) (test content '%xmltag))
		(xmleval content)
		(if (applicable? content) (content)
		    (xhtml content)))))))

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

