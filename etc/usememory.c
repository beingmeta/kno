#include <stdlib.h>
#include <stdio.h>
#include <string.h>

int main(int argc,char *argv[])
{
  int n_bytes, a_bytes=0, i=0, lim; char **vec;
  if (argc>1) n_bytes=strtol(argv[1],NULL,10);
  else n_bytes=256000000;
  lim=n_bytes/(512*1024);
  vec=malloc(sizeof(char *)*lim);
  while (i<lim) {
    char *data;
    /* fprintf(stderr,"Allocated %d bytes\n",a_bytes); */
    data=vec[i++]=malloc((512*1024));
    memset(data,1,(512*1024));
    a_bytes=a_bytes+(512*1024);}
  return 0;
}
