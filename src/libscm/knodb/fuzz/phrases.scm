;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/fuzz/phrases)

(use-module '{texttools varconfig logger knodb/fuzz/terms})

(define-init default-fuzz-term-opts #[])

(module-export! '{fuzz-phrases fuzz-phrase})

(define-init default-fragsize 1)
(varconfig! fuzz:term:fragsize default-fragsize)
(varconfig! fuzz:fragsize default-fragsize)

(defambda (fuzz-phrases terms (opts default-fuzz-term-opts)
			(language) (normcase) (dashchars) (stem) (soundslike) (fragsize))
  (default! language (getopt opts 'language 'en))
  (default! normcase (getopt opts 'normcase #t))
  (default! dashchars (getopt opts 'dashchars "-"))
  (default! stem (getopt opts 'stem #t))
  (default! soundslike (getopt opts 'soundslike #t))
  (default! fragsize (getopt opts 'fragsize (getopt opts 'frags default-fragsize)))
  (when (and fragsize (not (number? fragsize))) (set! fragsize 1))
  ;; The initial fuzz is empty, since we don't include the original terms but only fragments
  ;;  Those terms are included with the `phrasetext` option, which we handle at then end
  (local fuzz {})
  (set! terms (stdspace terms))
  (when normcase
    (when (or (overlaps? normcase '{#t default}) (default? normcase))
      (set+! terms (capitalize (pick terms somecap?)))
      (set+! terms (downcase terms)))
    (when (overlaps? normcase '{capitalize capit cap})
      (set+! terms (capitalize terms)))
    (when (overlaps? normcase '{lowercase lower})
      (set+! terms (downcase terms))))
  ;; Remove dash-type characters
  (when dashchars
    (set+! fuzz (textsubst terms (qc dashchars) " "))
    (set+! fuzz (textsubst terms (qc dashchars) "")))
  (local bases (basestring terms))
  (when fragsize
    (let* ((phrases (pick {terms bases} compound-string?))
	   (dashed (tryif dashchars (pick {terms bases} string-contains? dashchars)))
	   (phrasevecs (words->vector phrases))
	   (fuzzvecs {}))
      (set+! fuzz (vector->frags phrasevecs fragsize))
      (when stem
	(set+! fuzzvecs
	  (for-choices (vec phrasevecs)
	    (forseq (elt vec)
	      (choice (string->packet ((pick stem applicable?) elt opts))
		      (tryif (eq? language 'en) (string->packet (porter-stem elt))))))))
      (when soundslike
	(set+! fuzzvecs
	  (for-choices (vec phrasevecs)
	    (forseq (elt vec)
	      (choice (string->packet ((pick soundslike applicable?) elt opts))
		      (tryif (eq? language 'en)
			{(metaphone elt #t) (metaphone+ elt #t) (soundex elt #t)}))))))      
      (do-choices (fuzzvec fuzzvecs)
	(unless (or (position "" fuzzvec)  (position #"" fuzzvec) (position #{} fuzzvec))
	  (set+! fuzz (vector->frags fuzzvec fragsize))))
      (debug%watch "FUZZ-PHRASES" phrases fragsize
		   "nterms" (|| terms)  "nvecs" (|| fuzzvecs)
		   "fuzzcount" (|| fuzz))))
  (if (getopt opts 'phrasetext)
      {fuzz terms (string->packet (difference bases terms))}
      fuzz))

(define fuzz-phrase (fcn/alias fuzz-phrases))
