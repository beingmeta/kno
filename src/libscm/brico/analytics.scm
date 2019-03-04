;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/analytics)
;;; Basic term logic over BRICO

(use-module '{brico texttools})

;;; This module provides inference over the new BRICO semantics, which is
;;;  based on Aristotle's term logic.  The term logic provides four
;;;  basic relations between predicates:
;;;	relation	english				classic type
;;;	--------	-------				------------
;;;	(a)lways:	all humans are mortal		(A type)
;;;	(s)ometimes:	some humans are programmers	(E type)
;;;	(n)ever:	no humans are minerals		(I type)
;;;	(sn)=somenot:	some humans aren't programmers	(O type)
;;;
;;; These relationships are fairly simple to understand, align pretty
;;; well with common-sense usage, have simple and tractable rules
;;; of inference (the syllogisms), and have stood the test of time.
;;; It also delineates the bounds of possibility (what makes sense),
;;; which can be helpful for common sense reasonsing.

;;; They certainly aren't intended to represent *all* knowledge and
;;; certainly not include all possible inferences.  Term logic was
;;; chosen over more modern logics (e.g. FOPC or its variants) because
;;; it seemed better suited to common sense rather than being designed
;;; for mathematics.

;;; These can be extended (and will be) to represent particular other
;;; sorts of relationships.

;;; This file implements the basic inferences over these relationships
;;; and parts of this may eventually move into some faster foundation
;;; (i.e. C).

;;; We assume that some set of relations is primitively asserted
;;; between identified concepts and we seek to define operations
;;; against the space of inferred relations.  There are three
;;; operations we'll be implementing:
;;;  GET: for a concept P and relation % get all the Q such that P%Q
;;;  FIND: for a concept Q and relation % get all the P such that P%Q
;;;  TEST: for concepts P and Q and relation % is it the case that P%Q

;;;  There are four simple inferences that are implemented by the
;;;  GET/TEST methods and indexing over BRICO.  They are:

;;;   ALWAYS is transitive, if P always Q and Q always R, P always R
;;;    We implement this in indexing, so if P always R, we will find P
;;;    among (?? always R).
;;;   NEVER is symmetric, if P never Q then Q never P, this is
;;;    implemented by FramerD inverses.
;;;   SOMETIMES is symmetric, if P sometimes Q then Q sometimes P,
;;;    this is implemented with FramerD inverses.
;;;   SOMETIMES is implied by ALWAYS, if P always Q then P sometimes Q,
;;;    this is implemnted with FramerD multislots
;;;   SOMENOT is implied by NEVER, if P never Q then P somenot Q.

;;;   These also assume the assertion of relations through inverses,
;;;   so asserting Q always_inv P is the same as P always Q.

;;; Straight GET just returns relations as asserted;
;;; GET+ returns some inferred values controlled by the
;;;  third, TRYHARD, argument.  There are three levels
;;;  of inference:
;;;   TRYHARD=1 (#f)  get inferred values but don't bother with expanding ALWAYS
;;;   TRYHARD=2       get inferred value including those implied by ALWAYS, but
;;;                     don't do anything too computationally expensive
;;;   TRYHARD=3 (#t)  get all inferred values

;;; TRYING HARD

;;; A few of the inferences are potentially expensive, meaning that they
;;;  could involve iteration over a lot of possible cases, for example
;;;  a human is sometimes not a vegetarian because a human meat-eater is
;;;   always a human but never a vegetarian.  Determining this involves 
;;;   looking at all kinds of humans (or even all humans), which could be
;;;   pretty big and time consuming.  The methods below take a TRYHARD
;;;   argument which determines whether rules which might be time consuming
;;;   are tried.  This defaults to false (#f) except where there aren't any
;;;   expensive methods, where they default to true (#t).

;;; Judging whether a method will be expensive is based on assumptions about
;;;  the distributions of relations in the ontology.  We basically assume that
;;;  all the relationships except %always (the inverse of always) returns a
;;;  relatively small number of results (dozens) without inference.  A method
;;;  which iterates over any results of %always (?? always x), is considered
;;;  time consuming and will not be tried unless TRYHARD is true.  Likewise for
;;;  methods iterate over the composition of two relationships
;;;   (for instance, never and always).

;;; ALWAYS indexing

;;; Certain inferences can be built into the index for improved performance
;;;  at run time.  The key discriminant is that searches using the index
;;;  are common (e.g. part of other inferences) and that it only increase
;;;  the size of the index by a small linear constant.  For example,
;;;  sometimes(x) implies sometimes(a*(x)) so we can index the parents
;;;  a*(x) automatically; since we assume a small bound on the depth of the
;;;  always tree, this increases the index size by that small multiple.
;;;  Likewise, for never(x) implies never(a*(x)).

(define expanded-indexing #t)

;;; Index-based GET

;;; We can rely on the index to compute some values more quickly and with
;;;  less overhead than repeated loads and references.

(defambda (%getalways p) (?? @1/8{%always} p))
;; We rely on never being indexed symmetrically here.
(define (%getnever p) (?? @1/6{never} p))
;; We rely on never being indexed symmetrically here.
(define (%getsometimes p) (?? @1/5{sometimes} p))

;;; ALWAYS

(define (getalways p (tryhard 2))
  (if (= tryhard 1) (get p @1/4{always})
      (?? @1/8{/always} p)))

(define (testalways p q) (path? p always q))
;; Usually faster version using the index
(define (testalways p q) (overlaps? q (?? @1/8{/always} p)))

(define (findalways q (tryhard #t)) (?? always q))

;;; SOMETIMES

;;; Four ways that P sometimes Q
;;;  1) direct assertion (incl. as inverse or from always)
;;;  2) always through Q (P sometimes M and M always Q) [Darii]
;;;       a dog is sometimes running
;;;       running is always moving
;;;       a dog is sometimes moving
;;;  3) common descendant via sometimes and always, i.e. [Disamis,Datisi]
;;;      (M sometimes P and M always Q)
;;;       (If sometimes is stored symmetrically, (2)+(3) are the same
;;;        since M sometimes P would mean P sometimes M)
;;;         flying is sometimes dangerous
;;;         dangerous is always scary
;;;         flying is sometimes scary
;;;  4) common descendant for always (M always P and M always Q) [Darapti]
;;;         a singer-songwriter is always a singer
;;;         a singer-songwriter is always a songwriter
;;;         a singer is sometimes a songwriter

;;; (4) is the expensive one, since it requires finding the common
;;; descendant.  In the methods below, the tryhard argument determines
;;; whether the fourth case is considered.

(define (getsometimes p (tryhard 2))
  (choice (get p sometimes) ; case (1)
	  ;; case (2)+ (3 through get/sometimes symmetry)
	  (tryif (> tryhard 1)
		 (choice (%getalways (get p sometimes))
			 ;; While it would be easy to have sometimes
			 ;;  include /always (specls), that generates
			 ;;  a lot of stuff, so we make it a level 1 inference
			 (?? always (list p)))) 
	  ;; case (4)
	  (tryif (> tryhard 2)
		 (choice (%getalways (?? @1/4{always} p))
			 (?? always (list p))))))

(define (testsometimes p q)
  (or (if expanded-indexing
	  (or (overlaps? q (?? sometimes p))
	      (overlaps? p (?? sometimes q)))
	  (or (test p sometimes q)
	      (test (%getalways p) sometimes q)
	      (test p sometimes (%getalways q))))
      (exists? (?? @1/4{always} p @1/4{always} q))))

;; Get nodes with common children
(define (findsometimes q (tryhard 2))
  (choice (?? sometimes q)
	  (tryif (> tryhard 1) (?? @1/4{always} (list q)))
	  (tryif (> tryhard 2)
		 (let ((narrower (?? @1/4{always} q)))
		   (choice (?? sometimes narrower)
			   (findalways narrower))))))

;;; NEVER

;;; Four ways that P is never Q
;;;  1) direct assertion (incl. as inverse)
;;;  2) always . never (P always M, M never Q) [Celarent]
;;;       never inherits down always
;;;       if a surgeon is always a doctor
;;;          a doctor is never an idiot
;;;          a surgeon is never an idiot
;;;  3) never . always (P always M, Q never M) [Cesare]
;;;       never inherits down always
;;;       if a surgeon is always a doctor
;;;          an idiot is never a doctor
;;;          a surgeon is never an idiot
;;;  4) never . always_inv (P never M, Q always M) [Camestres]
;;;       inherits symmetrically
;;;       if an idiot is never a doctor
;;;          a  surgeon is always a doctor
;;;          an idiot is never a surgeon

(define (getnever p (tryhard 2))
  (choice (get p never) ;; case (1)
	  ;; case (2)+ (3 through get/never symmetry)
	  (tryif (> tryhard 1) (get (%getalways p) never)) 
	  ;; case (4)
	  (tryif (> tryhard 2) (?? always (get p never)))))
(define (testnever p q)
  (or (test p never q)
      (test (%getalways p) never q)
      (test p never (%getalways q))))

(define (findnever q (tryhard 2))
  (choice (?? never q) ; 1
	  (tryif (> tryhard 1) (?? never (%getalways q))) ; 4
	  (tryif (> tryhard 2) (?? always (get q never)))))

;;; SOMENOT

;;; Seven ways that P might not be Q
;;;  1) direct assertion (incl. from inverse and never)
;;;  2) sometimes, never (P is sometimes M,M is never Q) [Ferio]
;;;       a doctor is sometimes a criminal
;;;       a criminal is never a good guy
;;;       a doctor is sometimes not a good guy
;;;  3) sometimes, never_inv (P is sometimes M, Q is never M) [Festino]
;;;       (since never is implemented symmetrically, the same as 2)
;;;       a doctor is sometimes a criminal
;;;       a good guy is never a criminal
;;;       a doctor is sometimes not a good guy
;;;  4) somenot, always_inv (P is somenot M, Q is always M) [Baroco]
;;;       a doctor is sometimes not skilled
;;;       competent is always skilled
;;;       a doctor is sometimes not competent
;;;  5) always_inv, never (M is always P, M is never Q) [Felapton]
;;;       a poet is always an artist
;;;       a poet is never passionless
;;;       an artist is sometimes not passionless
;;;  6) always_inv, somenot (M is always P, M is somenot Q) [Bocardo]
;;;       a poet is always an artist
;;;       a poet is sometimes not passionless
;;;       an artist is sometimes not passionless
;;;  7) sometimes_inv, never (M is sometimes P, M never Q) [Ferison]
;;;       (since sometimes is implemented symmetrically, same as 2+3)
;;;       a poet is sometimes a teacher
;;;       a poet is never passionless
;;;       a teacher is sometimes not passionless

(define (getsomenot p (tryhard 2))
  (choice (get p somenot) ;; 1
	  (tryif (> tryhard 1) (get (get p sometimes) never)) ;; 2+3+7
	  (tryif (> tryhard 2)
		 (?? @1/4{always} (get p somenot))) ;; 4
	  (tryif (> tryhard 2)
		 (choice (get (?? @1/4{always} p) never)    ;; 5
			 (get (?? @1/4{always} p) somenot))) ;; 6
	  ))

(define (testsomenot p q)
  (choice (test p somenot q) ;; 1
	  (test (get p sometimes) never q) ;; 2+3+7
	  (test p somenot (getalways q)) ;; 4
	  (exists? (?? always p never q)) ;; 5
	  (exists? (?? always p somenot q)) ;; 6
	  ))

(define (findsomenot q (tryhard 2))
  (if (= tryhard 1)  (?? somenot q)
      ;; Should some of these be limited to tryhard=3?
      (choice (?? somenot q) ;; 1
	      (?? sometimes (?? never q)) ;; 2+3+7
	      (?? somenot (getalways q)) ;; 4
	      (getalways (?? never q))    ;; 5
	      (getalways (?? somenot q)) ;; 6
	      )))

;;; Commonly

(define (getcommonly p (tryhard 2))
  (if (> tryhard 1)
      (choice (get p commonly) (getalways (get p commonly)))
      (get p commonly)))

(define (findcommonly p (tryhard #t))
  (if tryhard
      (?? commonly p)
      (?? commonly (list p))))

(define (getrarely p (tryhard #f))
  (choice (get p rarely)
	  (tryif (> tryhard 2) (findalways (get p rarely)))))

(define (findrarely p (tryhard #f))
  (choice (?? rarely p)
	  (tryif tryhard (findalways (?? rarely p)))))

(module-export!
 '{getalways getnever getsometimes getsomenot getcommonly getrarely})
(module-export!
 '{%getalways %getnever %getsometimes %getsomenot})


;;; Get plus

(define (get+ concept slotid (level 2))
  ;; Convert booleans to numerics
  (if (eq? level #t) (set! level 3)
      (if (eq? level #f) (set! level 1)))
  (if (or (= level 1) (not (oid? slotid)))
      (get concept slotid)
      (if (test slotid 'type 'slot)
	  (case slotid
	    ((@1/4{always}) (getalways concept level))
	    ((@1/6{never}) (getnever concept level))
	    ((@1/5{sometimes}) (getsometimes concept level))
	    ((@1/7{somenot}) (getsomenot concept level))
	    ((@1/a{commonly}) (getcommonly concept level))
	    ((@1/b{rarely}) (getrarely concept level))
	    (else (get concept slotid)))
	  (get concept (choice slotid (?? @1/4{always} slotid))))))

(define (get++ concept slotid)
  (get+ concept slotid 3))

(module-export! '{get+ get++})

