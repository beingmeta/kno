;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

;;; Managing custom maps into BRICO
(in-module 'brico/maprules)

(use-module 'reflection)

(module-export! '{custom-map-name custom-map-language custom-map-handler})
(module-export! '{custom-get conform-maprule})

;;; A custom entry is a vector of the form:
;;;  #(name lang table)
;;; and rulesets are used to manage the replacement of entries
;;; with the same name.

(define custom-map-name first)
(define custom-map-language second)
(define custom-map-handler third)

(define (custom-get-list key language custom)
  (tryif (pair? custom)
	 (let* ((entry (car custom))
		(handler (custom-map-handler entry)))
	   (try
	    (if (custom-map-language entry)
		(tryif (eq? language (custom-map-language entry))
		       (if (table? handler) (get handler key)
			   (tryif (applicable? handler)
				  (handler key))))
		(if (table? handler)
		    (get handler (cons language key))
		    (tryif (applicable? handler)
			   (handler key language))))
	    (custom-get-list key language (cdr custom))))))

(defambda (custom-get key language custom)
  (if (or (fail? custom) (not custom) (null? custom))
      (fail)
      (if (pair? custom)
	  (custom-get-list key language custom)
	  (choice (get (custom-map-handler
			(pick custom custom-map-language language))
		       key)
		  (get (custom-map-handler
			(pick custom custom-map-language #f))
		       (cons language key))))))

;; This is used by config functions and massages a variety
;;  of specifications into a custom-map
(define (conform-maprule value)
  (if (vector? value) value
      (let* ((items (if (pair? value) (choice (car value) (cdr value))
			value))
	     (name (try (pick items symbol?) #f))
	     (language (try (pick items oid?) #f))
	     (map (try (difference items (choice name language)) #f)))
	(cond ((not (or (table? map) (applicable? map)))
	       (error 'config "Invalid map: " map))
	      ((and language (applicable? map)
		    (not (= (procedure-min-arity map) 1)))
	       (error 'config "map function requires too many arguments: "
		      map))
	      ((and (not language) (applicable? map)
		    (> (procedure-arity map) 1)
		    (not (= (procedure-min-arity map) 2)))
	       (error 'config "map function has wrong number of arguments: "
		      map))
	      (else (vector name language map))))))

;;; A custom entry is a vector of the form:
;;;  #(name lang table)
;;; and rulesets are used to manage the replacement of entries
;;; with the same name.

(define custom-map-name first)
(define custom-map-language second)
(define custom-map-handler third)

(define (custom-get-list key language custom)
  (tryif (pair? custom)
	 (let* ((entry (car custom))
		(handler (custom-map-handler entry)))
	   (try
	    (if (custom-map-language entry)
		(tryif (eq? language (custom-map-language entry))
		       (if (table? handler) (get handler key)
			   (tryif (applicable? handler)
				  (handler key))))
		(if (table? handler)
		    (get handler (cons language key))
		    (tryif (applicable? handler)
			   (handler key language))))
	    (custom-get-list key language (cdr custom))))))

(defambda (custom-get key language custom)
  (if (or (fail? custom) (not custom) (null? custom))
      (fail)
      (if (pair? custom)
	  (custom-get-list key language custom)
	  (choice (get (custom-map-handler
			(pick custom custom-map-language language))
		       key)
		  (get (custom-map-handler
			(pick custom custom-map-language #f))
		       (cons language key))))))

;; This is used by config functions and massages a variety
;;  of specifications into a custom-map
(define (conform-maprule value)
  (if (vector? value) value
      (let* ((items (if (pair? value) (choice (car value) (cdr value))
			value))
	     (name (try (pick items symbol?) #f))
	     (language (try (pick items oid?) #f))
	     (map (try (difference items (choice name language)) #f)))
	(cond ((not (or (table? map) (applicable? map)))
	       (error 'config "Invalid map: " map))
	      ((and language (applicable? map)
		    (not (= (procedure-min-arity map) 1)))
	       (error 'config "map function requires too many arguments: "
		      map))
	      ((and (not language) (applicable? map)
		    (> (procedure-arity map) 1)
		    (not (= (procedure-min-arity map) 2)))
	       (error 'config "map function has wrong number of arguments: "
		      map))
	      (else (vector name language map))))))

