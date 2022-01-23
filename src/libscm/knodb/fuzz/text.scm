;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/fuzz/text)

(use-module '{texttools varconfig logger})
(use-module '{knodb/fuzz knodb/fuzz/terms})

(define-init default-fuzz-text-opts #[])

(module-export! '{fuzz-text index-text})

(defambda (fuzz-text text (opts default-fuzz-text-opts) (language)
		     (stopwords) (stopchecks)
		     (normcase) (normpunct) (dashchars)
		     (soundslike) (stem) (fragsize)
		     (varcase) (markraw))
  (default! language (getopt opts 'language 'en))
  (default! stopwords (pickstrings (getopt opts 'stopwords {})))
  (default! stopchecks
    {(getopt opts 'stopchecks {})
     (pick (getopt opts 'stopwords {}) {applicable? hashset? hashtable?})})
  (default! normcase (getopt opts 'normcase #t))
  (default! normpunct (getopt opts 'normpunct #t))
  (default! dashchars (getopt opts 'dashchars "-"))
  (default! stem (getopt opts 'stem #t))
  (default! soundslike (getopt opts 'soundslike #t))
  (default! fragsize (getopt opts 'fragsize (getopt opts 'frags #default)))
  (when (and fragsize (not (number? fragsize))) (set! fragsize 1))
  (default! varcase (getopt opts 'varcase #f))
  (default! markraw (getopt opts 'raw #f))
  (local maxwordlen (getopt opts 'maxwordlen 16))
  (let* ((wordlist (getwords text))
	 (hashsets (pick stopchecks hashset?))
	 (index-words (reject (filter-choices (word (difference (elts wordlist) stopwords))
				(< 2 (length word) maxwordlen))
			stopchecks)))
    (fuzz-terms index-words opts language 
		normcase normpunct dashchars
		soundslike stem fragsize
		varcase markraw)))

(defambda (index-text index frame slotid (opts default-fuzz-text-opts) (text #f)
		      (stopwords) (stopchecks)
		      (language) (normcase) (normpunct) (dashchars)
		      (soundslike) (stem) (fragsize)
		      (varcase) (markraw))
  (default! language (getopt opts 'language 'en))
  (default! stopwords (pickstrings (getopt opts 'stopwords {})))
  (default! stopchecks
    {(getopt opts 'stopchecks {})
     (pick (getopt opts 'stopwords {}) {applicable? hashset? hashtable?})})
  (default! normcase (getopt opts 'normcase #t))
  (default! normpunct (getopt opts 'normpunct #t))
  (default! dashchars (getopt opts 'dashchars "-"))
  (default! stem (getopt opts 'stem #t))
  (default! soundslike (getopt opts 'soundslike #t))
  (default! fragsize (getopt opts 'fragsize (getopt opts 'frags #default)))
  (default! varcase (getopt opts 'varcase #f))
  (default! markraw (getopt opts 'raw #f))
  (when (and fragsize (not (number? fragsize))) (set! fragsize 1))
  (cond ((and (ambiguous? frame) text)
	 (do-choices (f frame)
	   (index-frame index f slotid
			(fuzz-text text opts language
				   stopwords stopchecks
				   normpunct normcase dashchars
				   soundslike stem fragsize
				   varcase markraw))))
	((ambiguous? frame)
	 (do-choices (f frame)
	   (index-frame index f slotid
			(fuzz-text (get f slotid) opts language
				   stopwords stopchecks
				   normcase normpunct dashchars
				   soundslike stem fragsize
				   varcase markraw))))
	(else (index-frame index frame slotid
			   (fuzz-text (or text (get frame slotid)) opts language
				      stopwords stopchecks
				      normcase normpunct dashchars
				      soundslike stem fragsize
				      varcase markraw)))))

