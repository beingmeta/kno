;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'kno/workpools)

;;; This provides macros for easy use of multiple threads (and cores)
;;; in applications.  It also provides a way to easily implement the
;;; prefetch/execute cycles which can improve performance on many
;;; database-intensive operations. 

(use-module '{reflection text/stringfmts varconfig logger defstruct fifo fifo/call kno/threads})

(defstruct (worktask annotated)
  subject
  (fn #f)
  (output #f)
  (status #f)
  (id (getuuid)))

(defstruct (workpool annotated)
  (fn #f)
  (output #f)
  (opts #f)
  (threads {})
  (fifo))

(define (workloop-run task pool)
  (unless (test task 'status 'done)
    (let ((output (or (worktask-output task) (workpool-output pool)))
	  (fn (or (worktask-fn task) (workpool-fn task)))
	  (subject (worktask-subject task)))
      (let ((v (fn subject)))
	(add! task 'status 'done)
	(cond ((applicable? output) (output (qc v)))
	      ((fifo? output)
	       (fifo/push/n! output (choice->vector v)))
	      ((workpool? output)
	       (workpool/add/n! output (choice->vector v)))
	      (else (let ((%loglevel (getopt opts 'loglevel %loglevel)))
		      (loginfo |WorkTaskResult| task ":\n  " v))))))))

(define (workpool-threadfn fifo workpool)
  (let ((item (fifo/pop fifo)))
    (while (exists? item)
      )))
