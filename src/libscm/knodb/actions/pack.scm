;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/pack)

(module-export! '{main pack})

(use-module '{logger text/stringfmts optimize})
(use-module '{knodb/actions/packpool knodb/actions/packindex})

(define %loglevel (config 'loglevel %notice%))

(define (opt-module name)
  (let ((mod (get-module name)))
    (optimize-module! (get mod '%optimize))
    (optimize-module! name)
    mod))

(define (usage)
  (lineout "Usage: pack <source> [target]\n"
    ($indented 4
	       "Repacks the database file stored in <from> into [to] (which defaults to packing in place)"
	       "Common options include:"
	       ($indented 4
			  "POOLTYPE=keep|kpool|filepool\n"
			  "COMPRESSION=none|zlib9|snappy|zstd9\n"
			  "CODESLOTS=yes|no\n"
			  "OVERWRITE=no|yes\n")
	       "For repacking pool files, common options include (first value is default) : \n"
	       ($indented 4
			  "POOLTYPE=keep|kpool|filepool\n"
			  "COMPRESSION=none|zlib9|snappy|zstd9\n"
			  "CODESLOTS=yes|no\n"
			  "OVERWRITE=no|yes\n")
	       "For repacking index files, common options include (first value is default) : \n"
	       ($indented 4
			  "INDEXTYPE=keep|kindex|filepool\n"
			  "OVERWRITE=no|yes\n")
	       "If specified, [to] must not exist unless OVERWRITE=yes")))

(define (pack (from #f) (to) (type (config 'type #f)))
  (default! to from)
  (when (overlaps? to {"inplace" "-"}) (set! to from))
  (cond ((and (string? from) (file-exists? from))
	 (if (if type (pool-type? type) (has-suffix from ".pool"))
	     ((get (opt-module 'knodb/actions/packpool) 'packpool) from to)
	     (if (if type (index-type? type) (has-suffix from ".index"))
		 ((get (opt-module 'knodb/actions/packindex) 'packindex) from to)
		 (usage))))
	(else (usage))))

(define main pack)
