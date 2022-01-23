;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/fuzz/terms)

(use-module '{texttools varconfig logger})

(define-init default-fuzz-term-opts #[])

(module-export! '{fuzz-terms fuzz-term index-terms})

(defambda (fuzz-terms terms (opts default-fuzz-term-opts) (language)
		      (normcase) (normpunct) (dashchars)
		      (soundslike) (stem) (fragsize)
		      (varcase) (markraw))
  (default! language (getopt opts 'language 'en))
  (default! normcase (getopt opts 'normcase #t))
  (default! normpunct (getopt opts 'normpunct #t))
  (default! dashchars (getopt opts 'dashchars "-"))
  (default! soundslike (getopt opts 'soundslike #t))
  (default! stem (getopt opts 'stem #t))
  (default! fragsize (getopt opts 'fragsize (getopt opts 'frags 1)))
  (when (and fragsize (not (and (integer? fragsize) (positive? fragsize))))
    (set! fragsize 1))
  (default! varcase (getopt opts 'varcase #f))
  (default! markraw (getopt opts 'raw #f))
  ;; Always normalize spacing
  (set! terms (stdspace terms))
  ;; This saves the raw un-normalized versions as `(list *term*)`.
  (local raw (tryif markraw (list terms)))
  (when normcase
    ;; Default normalization is to normalize capitalization.
    (when (or (overlaps? normcase '{#t default}) (default? normcase))
      (set+! terms (capitalize (pick terms somecap?)))
      (set+! terms (downcase terms)))
    (when (overlaps? normcase '{capitalize capit cap})
      (set+! terms (capitalize terms)))
    (when (overlaps? normcase '{lowercase lower})
      (set+! terms (downcase terms))))
  (when normpunct
    (set+! terms (if (table? normpunct)
  		     (stdpunct terms normpunct)
  		     (stdpunct terms))))
  ;; Initial fuzz is just the terms (stdspaced)
  (local fuzz {terms raw})
  ;; Basestring strings off any inflections. Bases are also used for fragment fuzzing.
  (local bases (basestring terms))
  ;; Add uninflected versions to the fuzz as packets
  (set+! fuzz (string->packet (difference bases terms)))
  (when varcase
    ;; This normalizes case but stores the results as packets in the fuzz to distinguish them
    (when (or (overlaps? varcase '{#t default}) (default? normcase))
      (set+! terms (string->packet (capitalize (pick terms somecap?))))
      (set+! terms (string->packet (downcase terms))))
    (when (overlaps? varcase '{capitalize capit cap})
      (set+! terms (string->packet (capitalize terms))))
    (when (overlaps? varcase '{lowercase lower})
      (set+! terms (string->packet (downcase terms)))))
  ;; Remove dash-type separators from the fuzz
  (when dashchars
    (set+! fuzz (textsubst terms (qc dashchars) " "))
    (set+! fuzz (textsubst terms (qc dashchars) "")))
  (when soundslike
    (set+! fuzz (string->packet ((pick soundslike applicable?) terms opts)))
    (cond ((eq? language 'en)
	   (set+! fuzz {(metaphone terms #t) (metaphone+ terms #t) (soundex terms #t)}))))
  (when stem
    (set+! fuzz (string->packet ((pick stem applicable?) terms opts)))
    (cond ((or (eq? language 'en) (and (oid? language) (test language 'langid 'en)))
  	   (set+! fuzz (string->packet (porter-stem terms))))
  	  ;; This is where we'll pull in stemmers, starting with hunspell
  	  ))
  (when fragsize
    (let* ((phrases (pick {terms bases} compound-string?))
  	   (dashed (tryif dashchars (pick {terms bases} string-contains? dashchars)))
  	   (phrasev (words->vector phrases))
  	   (dashedv (for-choices (dashed-term dashed)
  		      (->vector (textslice dashed-term (qc '(spaces) dashchars) 'drop))))
  	   (phrasevecs {phrasev dashedv})
  	   (phraseopts (getopt opts 'phraseopts #t))
  	   (fuzzvecs {}))
      (set+! fuzz (vector->frags phrasevecs fragsize))
      (when phraseopts
  	(when (and stem (or (eq? phraseopts #t) (overlaps? phraseopts 'stem)))
  	  (set+! fuzzvecs
  	    (for-choices (vec phrasevecs)
  	      (forseq (elt vec)
  		(choice (string->packet ((pick stem applicable?) elt opts))
  			(tryif (eq? language 'en) (string->packet (porter-stem elt))))))))
  	(when (and soundslike (or (eq? phraseopts #t) (overlaps? phraseopts 'soundslike)))
  	  (set+! fuzzvecs
  	    (for-choices (vec phrasevecs)
  	      (forseq (elt vec)
  		(choice (string->packet ((pick soundslike applicable?) elt opts))
  			(tryif (eq? language 'en)
  			  {(metaphone elt #t) (metaphone+ elt #t) (soundex elt #t)}))))))
  	(do-choices (fuzzvec fuzzvecs)
  	  (unless (or (position "" fuzzvec)  (position #"" fuzzvec) (position #{} fuzzvec))
  	    (set+! fuzz (vector->frags fuzzvec fragsize)))))))
  fuzz)

(define fuzz-term (fcn/alias fuzz-terms))

(defambda (index-terms index frame slotid (opts default-fuzz-term-opts)
		       (terms #f) (language)
		       (normcase) (normpunct) (dashchars)
		       (soundslike) (stem) (fragsize)
		       (varcase) (markraw))
  (default! language (getopt opts 'language 'en))
  (default! normcase (getopt opts 'normcase #t))
  (default! normpunct (getopt opts 'normpunct #t))
  (default! dashchars (getopt opts 'dashchars "-"))
  (default! soundslike (getopt opts 'soundslike #t))
  (default! stem (getopt opts 'stem #t))
  (default! fragsize (getopt opts 'fragsize (getopt opts 'frags 1)))
  (when (and fragsize (not (and (integer? fragsize) (positive? fragsize))))
    (set! fragsize 1))
  (default! varcase (getopt opts 'varcase #f))
  (default! markraw (getopt opts 'raw #f))

  (when (and fragsize (not (and (integer? fragsize) (positive? fragsize))))
    (set! fragsize 1))
  (cond ((and (ambiguous? frame) terms)
	 (do-choices (f frame)
	   (index-frame index f slotid
			(fuzz-terms terms opts language
				    normcase normpunct dashchars
				    soundslike stem
				    fragsize varcase markraw))))
	((ambiguous? frame)
	 (do-choices (f frame)
	   (index-frame index f slotid
			(fuzz-terms (get f slotid) opts language
				    normcase normpunct dashchars
				    soundslike stem
				    fragsize varcase markraw))))
	(else (index-frame index frame slotid
			   (fuzz-terms (or terms (get frame slotid)) opts language
				       normcase normpunct dashchars
				       soundslike stem
				       fragsize varcase markraw)))))

