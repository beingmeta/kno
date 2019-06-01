#!/usr/bin/kno
;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(config! 'indexinfer #f)

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (indexer/prefetch (qc oids))))

(define relations-index (target-file "relations.index"))
(define misc-slotids '{PERTAINYM REGION COUNTRY FAMILY LASTNAME})

(define done #f)
(define (bgcommit)
  (until done (sleep 15) (commit)))

(define (index-node f index)
  (index-relations index f)
  (index-refterms index f)
  (do-choices (slotid misc-slotids)
    (index-frame index f slotid (pickoids (get f slotid)))))

(defambda (indexer frames batch-state loop-state task-state)
  (let* ((index (get loop-state 'index))
	 (branch (index/branch index)))
    (prefetch-oids! frames)
    (do-choices (f frames) (index-node f branch))
    (branch/merge! branch)
    (swapout frames)))

(define (main . names)
  (let* ((pools (use-pool (try (elts names) brico-pool-names)))
	 (relns-index (target-index relations-index))
	 (isa-index (target-index "isa.index"))
	 (index  (make-aggregate-index {relns-index isa-index}
				       [register #t])))
    (engine/run indexer (difference (pool-elts pools) (?? 'source @1/1))
      `#[loop #[index ,index]
	 batchsize 2000 batchrange 3
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq 25])) )

(when (config 'optimize #t config:boolean)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize!))
