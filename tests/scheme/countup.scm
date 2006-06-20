(define (main (donefile "countup.done") (count 20) (interval 1))
  (dotimes (i count) (sleep interval) (message "Counting " i))
  (notify "Finished counting up to " count)
  (display (config 'sessionid) (open-output-file donefile)))


