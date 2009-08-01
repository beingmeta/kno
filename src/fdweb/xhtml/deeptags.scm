;;; -*- Mode: Scheme; Text-encoding: latin-1; -*-

(in-module 'xhtml/deeptags)

(use-module '{reflection texttools})
(use-module '{fdweb xhtml xhtml/clickit i18n})
(use-module 'deeptags)
(use-module '{brico brico/dterms})

(define oid-icon "http://static.beingmeta.com/graphics/diamond16.png")

(define (deeptag/span term oidlist)
  (span ((class "deeptag") )
    (span ((class "term")) term)
    (dolist (oid oidlist)
      (img SRC oid-icon ALT "+"
	   TITLE (get-single-gloss oid (get-language))))))

(define (deeptag/checkbox var term oidlist (checked {}))
  (span ((class "deeptag checkspan"))
    (span ((class "term"))
      (input TYPE "CHECKBOX" NAME var VALUE term
	     ("CHECKED" (if (or (overlaps? #t checked)
				(overlaps? term checked))
			    "CHECKED")))
      term)
    (dolist (oid oidlist)
      (span ((class "checkspan")
	     (ischecked (if (or (overlaps? #t checked) (overlaps? oid checked))
			    "yes")))
	(input TYPE "CHECKBOX" NAME var VALUE (oid->string oid term)
	       ("CHECKED" (if (or (overlaps? #t checked)
				  (overlaps? term checked))
			      "CHECKED")))
	(img SRC oid-icon ALT "+"
	     TITLE (get-single-gloss oid (get-language)))))))

(define (deeptags/edit var taglist checked)
  (let ((strings '())
	(oidmap (make-hashtable)))
    (doseq (tag taglist)
      (let ((string (tag->string tag)) (oid (tag->oid tag))))
      (if (test oidmap string)
	  (add! oidmap string oid)
	  (begin (set! strings (cons string strings))
		 (add! oidmap string oid))))
    (doseq (string (reverse strings) i)
      (if (> i 0) (xmlout " "))
      (deeptag/checkbox
       var string
       (append (sorted (intersection checked (get oidmap string)))
	       (sorted (intersection (get oidmap string) checked)))
       (qc checked)))))

(module-export! '{output-tagchecks deeptag/span deeptag/checkbox})
