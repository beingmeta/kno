#define main do_main
#include "fdexec.c"
#undef main

#define LOGMODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)

int main(int argc,char **argv)
{
  pid_t pid; u8_string base=u8_basename(argv[1],".scm");
  if (pid=fork()) {
    u8_string pid_file=u8_string_append(base,".pid",NULL);
    FILE *f=u8_fopen(pid_file,"w");
    u8_notify("Fork started","Launched process %d",pid);
    fprintf(f,"%d",pid);
    u8_fclose(f);
    exit(0);}
  else {
    u8_string log_file=u8_string_append(base,".log",NULL);
    u8_string err_file=u8_string_append(base,".err",NULL);
    int log_fd=u8_open_fd(log_file,O_WRONLY|O_APPEND|O_CREAT,LOGMODE);
    int err_fd=u8_open_fd(err_file,O_WRONLY|O_APPEND|O_CREAT,LOGMODE);
    dup2(log_fd,1); dup2(err_fd,2);
    return do_main(argc,argv);}
}


