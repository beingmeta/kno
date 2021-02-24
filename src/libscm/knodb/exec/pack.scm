;;; -*- Mode: Scheme -*-

(in-module 'knodb/exec/pack)

(module-export! '{main pack})

(use-module '{logger text/stringfmts})
(use-module '{knodb/exec/packpool knodb/exec/packindex})

(define %loglevel (config 'loglevel %notice%))

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
	     ((get (get-module 'knodb/exec/packpool) 'packpool) from to)
	     (if (if type (index-type? type) (has-suffix from ".index"))
		 ((get (get-module 'knodb/exec/packindex) 'packindex) from to)
		 (usage))))
	(else (usage))))



