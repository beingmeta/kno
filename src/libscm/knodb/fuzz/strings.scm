;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/fuzz/strings)

(use-module '{texttools varconfig logger})

(define-init default-fuzz-string-opts #[])

(module-export! '{fuzz-strings fuzz-string index-strings})

(defambda (fuzz-strings strings (opts default-fuzz-string-opts) 
			(normspace) (normcase) (normpunct) (dashchars)
			(separators) (fragsize))
  (default! normspace (getopt opts 'normspace #t))
  (default! normcase (getopt opts 'normcase #t))
  (default! normpunct (getopt opts 'normpunct #t))
  (default! dashchars (getopt opts 'dashchars #f))
  (default! separators (getopt opts 'separators #f))
  (default! fragsize (and separators (getopt opts 'fragsize 1)))
  (when normspace
    (set! strings (stdspace strings)))
  (when normcase
    (when (or (overlaps? normcase #t) (default? normcase))
      (set+! strings (capitalize (pick strings somecap?)))
      (set+! strings (downcase strings)))
    (when (overlaps? normcase 'capitalize)
      (set+! strings (capitalize strings)))
    (when (overlaps? normcase 'lowercase)
      (set+! strings (downcase strings))))
  (when normpunct
    (set+! strings (if (table? normpunct)
		       (stdpunct strings normpunct)
		       (stdpunct strings))))
  (local bases (basestring strings))
  ;; Initial fuzz is just the strings (stdspaced)
  (local fuzz strings)
  ;; Add any versions without inflection converted to packets
  (set+! fuzz (string->packet (difference bases strings)))
  ;; Remove dash-type characters
  (when dashchars
    (set+! fuzz (textsubst strings (qc dashchars) " "))
    (set+! fuzz (textsubst strings (qc dashchars) "")))
  (when separators
    (do-choices (separator separators)
      (let* ((vec (textslice {strings bases} separator))
	     (frags (vector->frags vec fragsize #t)))
	(set+! fuzz frags))))
  fuzz)

(define fuzz-string (fcn/alias fuzz-strings))

(defambda (index-strings index frame slotid (opts default-fuzz-string-opts) (strings #f)
			 (normspace) (normcase) (normpunct) (dashchars)
			 (separators) (fragsize))
  (default! normspace (getopt opts 'normspace #t))
  (default! normcase (getopt opts 'normcase #t))
  (default! normpunct (getopt opts 'normpunct #t))
  (default! dashchars (getopt opts 'dashchars #f))
  (default! separators (getopt opts 'separators #f))
  (default! fragsize (and separators (getopt opts 'fragsize 1)))
  (cond ((and (ambiguous? frame) strings)
	 (do-choices (f frame)
	   (index-frame index f slotid 
			(fuzz-strings strings opts 
				      normspace normcase normpunct dashchars
				      separators fragsize))))
	((ambiguous? frame)
	 (do-choices (f frame)
	   (index-frame index f slotid 
			(fuzz-strings (get f slotid) opts
				      normspace normcase normpunct dashchars
				      separators fragsize))))
	(else (index-frame index frame slotid
			   (fuzz-strings (or strings (get frame slotid)) opts
					 normspace normcase normpunct dashchars
					 separators fragsize)))))

