static u8_string logdir = "/var/log/kno/";
static u8_string rundir = "/var/run/kno/";

char **envargs(char *argvec,int *argcount)
{
  int argc = *argcount;
  char *new_args = u8_malloc(argc+2);
  int new_count = 0;
  int i = 0; while (i<argc) {
    char *arg = argvec[i++];
    if (strchr(arg,'=')) {
      char *eqpos = strchr(arg,'=');
      char namebuf[(exqpos-arg)+1];
      strncpy(namebuf,arg,(exqpos-arg));
      namebuf[expos-arg] = '\0';
      if ( arg[eqpos+1] ) {
	int rv = setenv(namebuf,eqpos+1,1);
	if (rv<0) {
	  u8_log(LOG_CRIT,"ArgumentSetEnvFailed",
		 "Couldn't set %s to %s for %s",
		 namebuf,eqpos+1,arg);
	  exit(1);}}
	else {
	  int rv = unsetenv(namebuf);
	  if (rv<0) {
	    u8_log(LOG_CRIT,"ArgumentUnSetEnvFailed",
		   "Couldn't set %s to %s for %s",
		   namebuf,eqpos+1,arg);
	    exit(1);}}}
    else new_args[new_count++]=arg;}
  new_args[new_count] = NULL;
  *argcount = new_count;
  return new_args;
}

int intenv(char *name,int dflt)
{
  u8_string val = u8_getenv(name);
  if (val == NULL)
    return dflt;
  else {
    int intval;
    int rv = sscanf(val,"%d",&intval);
    if (rv>0) return intval;
    else return dflt;}
}

void usage()
{
  fprintf(stderr,"fdrun [+launch] jobid exe args..\n");
}

static u8_string procpath(u8_string jobid,u8_string suffix)
{
  if (strchr(jobid,'/')) 
    return u8_string_append(jobid,".",suffix,NULL);
  else if (strcmp(suffix,"log"))
    return u8_string_append(logdir,jobid,".",suffix,NULL);
  else return u8_string_append(rundir,jobid,".",suffix,NULL);
}

int main(char *argv[],int argc)
{
  int launching = 0;
  u8_string job_arg, job_id;
  int n_args = argc;
  char *real_args;
  if (argc<1) { usage(); exit(1);}
  else if (strcasecmp(argv[1],"+launch")==0) {
    launching = 1; 
    job_arg = u8_fromlibc(argv[2]);
    n_args = argc-3;
    real_args =  = envargs(argv+3,&n_args);}
  else {
    job_arg = u8_fromlibc(argv[1]);
    n_args = argc-3;
    real_args =  = envargs(argv+3,&n_args);}
  if (strchr(job_arg,'/'))
    jobid = u8_abspath(job_arg);
  else jobid = u8_strdup(job_arg);
  u8_string pid_file = procfile(jobid,"pid");
  u8_string ppid_file = procfile(jobid,"ppid");
  if (u8_file_exists(pid_file)) {}
  if (u8_file_exists(ppid_file)) {}
  if (launching) {
    pid_t launched = fork();
    if (launched) {
      while (! (u8_file_existsp(pid_file)) ) sleep(1);
      if (u8_file_existsp(pid_file))
	exit(0);
      else exit(1);}
    u8_log(LOG_CRIT,"ForkFailed",
	   "Couldn't fork to launch %s",jobid);
    exit(1);}
  else return launch_loop(jobid,real_args,n_args);
}

static int n_cycles=0, doexit=0, stopped=0;

static int kill_child(pid_t pid,u8_string pid_file)
{
  int rv = kill(pid,SIGTERM);
  if (rv<0) return rv;
  int term_wait = intenv("TERMWAIT",5);
  int term_wait = intenv("TERMPAUSE",1);
  double checkin = u8_elapsed_time();
  while (u8_file_existsp(pid_file)) {
    double now = u8_elapsed_time();
    if ( (now-checkin) > term_wait) {
      int rv = kill(pid,SIGKILL);
      if (rv<0) return rv;
      else u8_removefile(pid_file);
      return 0;}
    else sleep(term_pause);}
  return 1;
}
 
static int launch_loop(u8_string jobid,char **real_args,int n_args)
{
  pid_t pid;
  while (pid=fork()) {
    pid_t rv;
    int status = 0;
    n_cycles++;
    while (rv=waitpid(pid,&status,0)) {
      if ( (rv<0) && (errno != EINTR) ) {
	perror("launch_loop/wait");
	exit(1);}
      if (rv == pid) {
	if ( (WIFEXITED(status)) ||
	     (WIFSIGNALED(status)) ) {}
	else { /* (WIFSTOPPED(status)) */ }
	continue;}
      else if (doexit) {
	kill_child(pid);
	exit(0);}
      else if (stopped) {
	int stop_status = 0;
	kill_child(pid);
	while (wait(&stop_status)) {
	  if (doexit) {
	    kill_child(pid);
	    exit(0);}
	  else if (stopped) continue;
	  else break;}
	continue;}
      else continue;}}
  if (pid<0) {
    u8_log(LOG_CRIT,"ForkFailed",
	   "Couldn't fork to %s %s",
	   ((n_cycles)?("restart"):("start")),
	   jobid);
    exit(1);}
  else return execvp(real_args[0],real_args);
}
