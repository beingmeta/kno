;;; -*- Mode: Scheme; -*-

(use-module '{logger varconfig fifo engine flexdb stringfmts ezrecords})
(use-module '{flexdb flexdb/branches})

(config! 'cachelevel 2)
(config! 'thread:logexit #f)

(define indir (config 'indir (abspath "brico/")))
(define outdir (config 'outdir (abspath "fresh/")))
(define bugjar (abspath "bugjar/"))

(unless (file-directory? bugjar) (mkdir bugjar))
(config! 'engine:bugjar bugjar)

(define brico-pool-names "brico.pool")

(define (check-dirs)
  (unless (file-directory? indir)
    (logpanic |BadInputDir|
      "The specified input path " (write indir) " isn't a directory"))
  (unless (file-directory? outdir)
    (logpanic |BadOutputDir|
      "The specified output path " (write outdir) " isn't a directory")))
(when (config 'checkdirs #t config:boolean) (check-dirs))

;;(define misc-slotids (file->dtype (mkpath data-dir "miscslots.dtype")))

(config! 'bricosource indir)
(pool/ref (mkpath indir "brico.pool"))

(use-module '{brico brico/indexing})
(use-module '{brico brico/indexing mttools trackrefs optimize tinygis})
(use-module '{logger varconfig})

(defrecord (langinfo)
  language id norm aliases frag cue gloss)

(define lang-slots
  (let ((table (make-hashtable)))
    (do-choices (lang (pickoids all-languages))
      (store! table {lang (get lang 'key)}
	      (cons-langinfo lang (get lang 'key) 
			     (get norm-map lang)
			     (get alias-map lang)
			     (get frag-map lang)
			     (get indicator-map lang)
			     (get gloss-map lang))))))

(define (make-threadindex base)
  (let ((tmp (frame-create #f)))
    (do-choices (slotid (getkeys base))
      (store! tmp slotid (make-hashtable)))
    tmp))

(define (threadindex/merge! into from)
  (do-choices (slotid (getkeys from))
    (index/merge! (try (get into slotid) (get into '%default)) 
		  (get from slotid))))

(define dontindex (choice (?? 'source @1/1)))

(define (target-file name) (mkpath outdir name))

(define (writable-index . args)
  (let ((ix (apply open-index args)))
    (indexctl ix 'readonly #f)
    ix))

(defambda (target-index filename (opts #f) (size) (keyslot))
  (default! size (getopt opts 'size (config 'INDEXSIZE (* 8 #mib))))
  (default! keyslot (getopt opts 'keyslot (config 'keyslot #f)))
  (when (not size) (set! size (* 8 #mib)))
  (unless (position #\/ filename) 
    (set! filename (mkpath outdir filename)))
  (cond ((and (file-exists? filename) (not (config 'REBUILD #f config:boolean)))
	 (writable-index filename
			 `(#[register ,(getopt opts 'register #t)]
			   . ,opts)))
	(else (when (file-exists? filename) 
		(logwarn |ReplacingFile| 
		  (write filename) ", backup in " 
		  (write  (glom filename ".bak")))
		(move-file filename (glom filename ".bak")))
	      (make-index filename
		`(#[type hashindex size ,size keyslot ,keyslot] . ,opts))
	      (lognotice |NewIndex| "Making new file index " filename)
	      (writable-index filename
			      `(#[register ,(getopt opts 'register #t)]
				. ,opts)))))
