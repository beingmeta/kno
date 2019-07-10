(in-module 'splitmod)

(use-module 'logger)

(if (config 'splitmod:err)
    (error NotThisModule Badmod)
    (logwarn |PerfectlyFine| "Here in splitmodsville"))



