;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/pack)

(module-export! '{main pack})

(use-module '{logger text/stringfmts optimize})
(define %optmods
  '{knodb/actions/packpool knodb/actions/packindex
    knodb/actions/flexpack})

;;(use-module '{knodb/actions/packpool knodb/actions/packindex})

(define %loglevel (config 'loglevel %notice%))

(defimport packpool 'knodb/actions/packpool)
(defimport packindex 'knodb/actions/packindex)
(defimport flexpack 'knodb/actions/flexpack)

(define (usage)
  (lineout "Usage: pack <source> [target]\n"
    ($indented 4
	       "Repacks the database file stored in <source> into [to] (which defaults to packing in place)"
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
			  "OVERWRITE=no|yes\n")
	       "For repacking index files, common options include (first value is default) : \n"
	       ($indented 4
			  "INDEXTYPE=keep|kindex|filepool\n"
			  "OVERWRITE=no|yes\n")
	       "If specified, [to] must not exist unless OVERWRITE=yes")))

(define (pack (from #f) (to) . args)
  (default! to from)
  (when (overlaps? to {"inplace" "-"}) (set! to from))
  (cond ((and (string? from) (file-exists? from))
	 (cond ((has-suffix from ".pool")
		(packpool from to))
	       ((has-suffix from ".index")
		(packindex from to))
	       ((has-suffix from ".flexindex")
		(apply flexpack from (basename to #t) args))
	       ((file-directory? from))
	       (else (usage))))
	(else (usage))))

(define main pack)
