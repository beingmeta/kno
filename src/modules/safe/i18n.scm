(in-module 'i18n)

(use-module '{brico texttools reflection})

(module-export! '{i18n/translate i18n/translator i18n/translateout})

;;;; Variables and configuration

;; This is the default method for getting the current language.  For
;; web stuff, this should use (get-language).
(define (defaultlanguagefn)
  (try (threadget 'language) default-language))

;; This is configurable
(define getlanguagefn defaultlanguagefn)

(define (i18n/langfn-config var (value))
  (cond ((not (bound? value)) getlanguagefn)
	((applicable? value) (set! getlanguagefn value))
	(else (error "Invalid i18n/out languagefn"))))
(config-def! 'i18n/langfn i18n/langfn-config)

;;;; Tables

;; Tables with keys of the form (inlang . translation)
;;  and values of the form (outlang . translation)
(define translations {})
;; Set of functions going from <object,lang> to strings
(define translators {})
;; A hashset of requested translations
(define need-translations (make-hashset))

(define (load-translations file)
  (let* ((table (make-hashtable))
	 (in (open-input-file file))
	 (entry (read in)))
    (until (eof-object? entry)
      (let ((head (car entry))
	    (key (cons (car head) (cadr head))))
	(doseq (xlation (cdr entry))
	  (add! table key (cons (car xlation) (cdr xlation)))))
      (set! entry (read in)))
    table))

(define (translations-config var (value))
  (cond ((not (bound? value)) translations)
	((table? value) (set+! translations value))
	((string? value)
	 (set+! translations (load-translations value)))
	(else (error "Invalid translations spec"))))
(config-def! 'i18n/translations translations-config)

(define (translators-config var (value))
  (cond ((not (bound? value)) translators)
	((applicable? value) (set+! translators value))
	(else (error "Invalid translations spec"))))
(config-def! 'i18n/translators translators-config)

;;; Generic translation using tables

(define (translate item from (to (getlanguagefn)) (domain #f))
  (try (pick-one (get (get translations (if domain
					    (cons* from domain item)
					    (cons from item)))
		      to))
       (tryif (not (or (pair? item) (string? item)))
	      (for-choices (translator translators)
		(translator item to)))
       (begin (hashset-add! need-translations
			    (choice (cons from item)
				    (cons (cons from to) item)))
	      item)))

(define i18n/translate translate)

;;; Generating translators

(define (i18n/translator inlang)
  (lambda (item (outlang (getlanguagefn)))
    (translate item inlang outlang)))

(define (i18n/translateout outmethod inlang)
  (macro expr
    (if (= (length expr) 2)
	`(,translate ,(cadr expr) ',inlang)
	`(case (,getlanguagefn)
	   ,@(map (lambda (lang.expr)
		    `((,(car lang.expr)) (,outmethod ,@(cdr lang.expr))))
		  (->list
		   (sorted (get translations (cons inlang (cdr expr))) car)))
	   (else (,hashset-add! ,need-translations
				(choice '(,inlang ,@(cdr expr))
					(cons (cons ',inlang (,getlanguagefn))
					      ',(cdr expr))))
		 (,outmethod ,@(map (lambda (ex) `(,translate ,ex ',inlang))
				    (cdr expr))))))))






