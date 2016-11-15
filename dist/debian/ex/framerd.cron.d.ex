#
# Regular cron jobs for the framerd package
#
0 4	* * *	root	[ -x /usr/bin/framerd_maintenance ] && /usr/bin/framerd_maintenance
