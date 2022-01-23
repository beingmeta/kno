;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/search)

(use-module '{fifo engine text/stringfmts logger varconfig})
(use-module '{knodb knodb/fuzz knodb/fuzz/terms knodb/fuzz/strings})

(module-export! '{knodb/index+! knodb/index-graph! knodb/index*!})

(define-init %loglevel %notice%)

(define-init fuzztypes #[])

(define genls @1/2c272{genls})
(define genls* @1/2c27b{genls*})
(define specls* @1/2c27c{kinds*})

(defambda (knodb/index+! index frame slotid (fuzz) (opts #f) (values) (graph.index))
  (default! fuzz
    (try (get (getopt opts 'fuzz {}) slotid)
	 (get (dbctl index 'metadata 'fuzz) slotid)
	 (tryif (oid? slotid) (get slotid 'fuzz))
	 (get fuzztypes slotid)
	 #f))
  (default! graph.index (getopt opts 'graphindex index))
  (default! values (get frame slotid))
  (local fuzztype (if (pair? fuzz) (car fuzz) fuzz))
  (let ((oids (pickoids values)))
    (when (exists? oids)
      (set! values (difference values oids))
      (knodb/index-graph! graph.index frame slotid oids fuzz opts))
    (when (exists? values)
      (index-frame index frame slotid
		   (cond ((not fuzz) values)
			 ((eq? fuzztype 'terms)
			  (fuzz-terms values (if (pair? fuzz) (cdr fuzz) #default)))
			 ((eq? fuzztype 'string)
			  (fuzz-terms values (if (pair? fuzz) (cdr fuzz) #default)))
			 (else (knodb/fuzz values fuzz opts)))))))

(defambda (knodb/index-graph! index frame slotid (opts #f)
			      (inverse) (genslot) (values #f))
  (default! inverse
    (getopt opts 'inverse (and (oid? slotid) (try (get slotid 'inverse) #f))))
  (default! genslot
    (getopt opts 'genslot (and (oid? slotid) (try (get slotid 'genls) genls*))))
  (cond ((and (not values) (ambiguous? frame))
	 (do-choices (f frame)
	   (knodb/index-graph! f slotid opts inverse genslot (get f slotid))))
	((ambiguous? frame)
	 (do-choices (f frame) 
	   (knodb/index-graph! f slotid opts inverse genslot (get f slotid))))
	(else
	 (set! values (pickoids (get frame slotid)))
	 (index-frame index frame slotid values)
	 (when inverse (index-frame index values inverse frame))
	 (when (exists? genslot) (index-frame index frame slotid (list values)))
	 (do-choices (g genslot)
	   (cond ((eq? g genls*)
		  (index-frame index frame slotid (?? specls* values)))
		 (else (index-frame index frame slotid
				    (pickoids (get values g)))))))))

(define (knodb/index*! index frame slot* slot (inverse) (v))
  (default! inverse (get slot 'inverse))
  (default! v (get frame slot))
  (index-frame index frame slot v)
  (index-frame index frame slot* v)
  (when inverse (index-frame index (pickoids v) inverse frame))
  (do ((parents v (difference (get parents slot) seen))
       (seen frame (choice parents seen)))
      ((empty? parents))
    (index-frame index frame slot* parents)
    (when inverse (index-frame index (pickoids parents) inverse frame))))


