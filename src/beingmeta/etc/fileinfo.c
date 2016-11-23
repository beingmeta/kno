#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

#define BLOCK_SIZE 8192

/*
 * sha1.c
 *
 * Originally witten by Steve Reid <steve@edmweb.com>
 * 
 * Modified by Aaron D. Gifford <agifford@infowest.com>
 *
 * NO COPYRIGHT - THIS IS 100% IN THE PUBLIC DOMAIN
 *
 * The original unmodified version is available at:
 *    ftp://ftp.funet.fi/pub/crypt/hash/sha/sha1.c
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR(S) AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR(S) OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * sha.h
 *
 * Originally taken from the public domain SHA1 implementation
 * written by by Steve Reid <steve@edmweb.com>
 * 
 * Modified by Aaron D. Gifford <agifford@infowest.com>
 *
 * NO COPYRIGHT - THIS IS 100% IN THE PUBLIC DOMAIN
 *
 * The original unmodified version is available at:
 *    ftp://ftp.funet.fi/pub/crypt/hash/sha/sha1.c
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR(S) AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR(S) OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
/* Define this if your machine is LITTLE_ENDIAN, otherwise #undef it: */
#ifndef LITTLE_ENDIAN
#define LITTLE_ENDIAN 1
#endif

/* Make sure you define these types for your architecture: */
typedef unsigned int sha1_quadbyte;	/* 4 byte type */
typedef unsigned char sha1_byte;	/* single byte type */

/*
 * Be sure to get the above definitions right.  For instance, on my
 * x86 based FreeBSD box, I define LITTLE_ENDIAN and use the type
 * "unsigned long" for the quadbyte.  On FreeBSD on the Alpha, however,
 * while I still use LITTLE_ENDIAN, I must define the quadbyte type
 * as "unsigned int" instead.
 */

#define SHA1_BLOCK_LENGTH	64
#define SHA1_DIGEST_LENGTH	20

/* The SHA1 structure: */
typedef struct _SHA_CTX {
	sha1_quadbyte	state[5];
	sha1_quadbyte	count[2];
	sha1_byte	buffer[SHA1_BLOCK_LENGTH];
} SHA_CTX;

#ifndef NOPROTO
void SHA1_Init(SHA_CTX *context);
void SHA1_Update(SHA_CTX *context, sha1_byte *data, unsigned int len);
void SHA1_Final(sha1_byte digest[SHA1_DIGEST_LENGTH], SHA_CTX* context);
#else
void SHA1_Init();
void SHA1_Update();
void SHA1_Final();
#endif

#define rol(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

/* blk0() and blk() perform the initial expand. */
/* I got the idea of expanding during the round function from SSLeay */

#ifdef LITTLE_ENDIAN
#define blk0(i) (block->l[i] = (rol(block->l[i],24)&(sha1_quadbyte)0xFF00FF00) \
	|(rol(block->l[i],8)&(sha1_quadbyte)0x00FF00FF))
#else
#define blk0(i) block->l[i]
#endif

#define blk(i) (block->l[i&15] = rol(block->l[(i+13)&15]^block->l[(i+8)&15] \
	^block->l[(i+2)&15]^block->l[i&15],1))

/* (R0+R1), R2, R3, R4 are the different operations used in SHA1 */
#define R0(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk0(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R1(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R2(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0x6ED9EBA1+rol(v,5);w=rol(w,30);
#define R3(v,w,x,y,z,i) z+=(((w|x)&y)|(w&x))+blk(i)+0x8F1BBCDC+rol(v,5);w=rol(w,30);
#define R4(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0xCA62C1D6+rol(v,5);w=rol(w,30);

typedef union _BYTE64QUAD16 {
	sha1_byte c[64];
	sha1_quadbyte l[16];
} BYTE64QUAD16;

/* Hash a single 512-bit block. This is the core of the algorithm. */
void SHA1_Transform(sha1_quadbyte state[5], sha1_byte buffer[64]) {
	sha1_quadbyte	a, b, c, d, e;
	BYTE64QUAD16	*block;

	block = (BYTE64QUAD16*)buffer;
	/* Copy context->state[] to working vars */
	a = state[0];
	b = state[1];
	c = state[2];
	d = state[3];
	e = state[4];
	/* 4 rounds of 20 operations each. Loop unrolled. */
	R0(a,b,c,d,e, 0); R0(e,a,b,c,d, 1); R0(d,e,a,b,c, 2); R0(c,d,e,a,b, 3);
	R0(b,c,d,e,a, 4); R0(a,b,c,d,e, 5); R0(e,a,b,c,d, 6); R0(d,e,a,b,c, 7);
	R0(c,d,e,a,b, 8); R0(b,c,d,e,a, 9); R0(a,b,c,d,e,10); R0(e,a,b,c,d,11);
	R0(d,e,a,b,c,12); R0(c,d,e,a,b,13); R0(b,c,d,e,a,14); R0(a,b,c,d,e,15);
	R1(e,a,b,c,d,16); R1(d,e,a,b,c,17); R1(c,d,e,a,b,18); R1(b,c,d,e,a,19);
	R2(a,b,c,d,e,20); R2(e,a,b,c,d,21); R2(d,e,a,b,c,22); R2(c,d,e,a,b,23);
	R2(b,c,d,e,a,24); R2(a,b,c,d,e,25); R2(e,a,b,c,d,26); R2(d,e,a,b,c,27);
	R2(c,d,e,a,b,28); R2(b,c,d,e,a,29); R2(a,b,c,d,e,30); R2(e,a,b,c,d,31);
	R2(d,e,a,b,c,32); R2(c,d,e,a,b,33); R2(b,c,d,e,a,34); R2(a,b,c,d,e,35);
	R2(e,a,b,c,d,36); R2(d,e,a,b,c,37); R2(c,d,e,a,b,38); R2(b,c,d,e,a,39);
	R3(a,b,c,d,e,40); R3(e,a,b,c,d,41); R3(d,e,a,b,c,42); R3(c,d,e,a,b,43);
	R3(b,c,d,e,a,44); R3(a,b,c,d,e,45); R3(e,a,b,c,d,46); R3(d,e,a,b,c,47);
	R3(c,d,e,a,b,48); R3(b,c,d,e,a,49); R3(a,b,c,d,e,50); R3(e,a,b,c,d,51);
	R3(d,e,a,b,c,52); R3(c,d,e,a,b,53); R3(b,c,d,e,a,54); R3(a,b,c,d,e,55);
	R3(e,a,b,c,d,56); R3(d,e,a,b,c,57); R3(c,d,e,a,b,58); R3(b,c,d,e,a,59);
	R4(a,b,c,d,e,60); R4(e,a,b,c,d,61); R4(d,e,a,b,c,62); R4(c,d,e,a,b,63);
	R4(b,c,d,e,a,64); R4(a,b,c,d,e,65); R4(e,a,b,c,d,66); R4(d,e,a,b,c,67);
	R4(c,d,e,a,b,68); R4(b,c,d,e,a,69); R4(a,b,c,d,e,70); R4(e,a,b,c,d,71);
	R4(d,e,a,b,c,72); R4(c,d,e,a,b,73); R4(b,c,d,e,a,74); R4(a,b,c,d,e,75);
	R4(e,a,b,c,d,76); R4(d,e,a,b,c,77); R4(c,d,e,a,b,78); R4(b,c,d,e,a,79);
	/* Add the working vars back into context.state[] */
	state[0] += a;
	state[1] += b;
	state[2] += c;
	state[3] += d;
	state[4] += e;
	/* Wipe variables */
	a = b = c = d = e = 0;
}


/* SHA1_Init - Initialize new context */
void SHA1_Init(SHA_CTX* context) {
	/* SHA1 initialization constants */
	context->state[0] = 0x67452301;
	context->state[1] = 0xEFCDAB89;
	context->state[2] = 0x98BADCFE;
	context->state[3] = 0x10325476;
	context->state[4] = 0xC3D2E1F0;
	context->count[0] = context->count[1] = 0;
}

/* Run your data through this. */
void SHA1_Update(SHA_CTX *context, sha1_byte *data, unsigned int len) {
	unsigned int	i, j;

	j = (context->count[0] >> 3) & 63;
	if ((context->count[0] += len << 3) < (len << 3)) context->count[1]++;
	context->count[1] += (len >> 29);
	if ((j + len) > 63) {
	    memcpy(&context->buffer[j], data, (i = 64-j));
	    SHA1_Transform(context->state, context->buffer);
	    for ( ; i + 63 < len; i += 64) {
	        SHA1_Transform(context->state, &data[i]);
	    }
	    j = 0;
	}
	else i = 0;
	memcpy(&context->buffer[j], &data[i], len - i);
}


/* Add padding and return the message digest. */
void SHA1_Final(sha1_byte digest[SHA1_DIGEST_LENGTH], SHA_CTX *context) {
	sha1_quadbyte	i, j;
	sha1_byte	finalcount[8];

	for (i = 0; i < 8; i++) {
	    finalcount[i] = (sha1_byte)((context->count[(i >= 4 ? 0 : 1)]
	     >> ((3-(i & 3)) * 8) ) & 255);  /* Endian independent */
	}
	SHA1_Update(context, (sha1_byte *)"\200", 1);
	while ((context->count[0] & 504) != 448) {
	    SHA1_Update(context, (sha1_byte *)"\0", 1);
	}
	/* Should cause a SHA1_Transform() */
	SHA1_Update(context, finalcount, 8);
	for (i = 0; i < SHA1_DIGEST_LENGTH; i++) {
	    digest[i] = (sha1_byte)
	     ((context->state[i>>2] >> ((3-(i & 3)) * 8) ) & 255);
	}
	/* Wipe variables */
	i = j = 0;
	memset(context->buffer, 0, SHA1_BLOCK_LENGTH);
	memset(context->state, 0, SHA1_DIGEST_LENGTH);
	memset(context->count, 0, 8);
	memset(&finalcount, 0, 8);
}

static char hexdigits[16]="0123456789ABCDEF";

int main(int argc,char *argv[])
{
  if (argc<2) {
    fprintf(stderr,"Usage: fileinfo 'filename' [root]\n");
    exit(1);}
  else {
    char *filename=argv[1], *root=((argc<3)?(NULL):(argv[2]));
    char *abspath=NULL, *relpath=NULL;
    struct stat info; time_t mtime; struct tm *modtime, modtime_buf;
    unsigned char *content, *ptr, *original;
    int retval=stat(filename,&info);
    int i=0, file_len, bytes_read=0, abs_len;
    SHA_CTX context;
    sha1_byte digest[SHA1_DIGEST_LENGTH];
    unsigned char digest_string[41];
    FILE *f;
    if (retval<0) {
      fprintf(stderr,"STAT error %s\n",strerror(errno));
      exit(1);}
    else if (!(f=fopen(filename,"r"))) {
      fprintf(stderr,"FOPEN error %s\n",strerror(errno));
      exit(1);}
    mtime=info.st_mtime;
    memset(&modtime_buf,0,sizeof(struct tm));
    memset(digest,0,sizeof(digest));
    modtime=gmtime_r(&mtime,&modtime_buf);
    if ((*filename=='.')&&(filename[1]=='/')) filename=filename+2;
    if (*filename=='/') {
      int name_len=strlen(filename);
      abspath=malloc(name_len+1);
      strncpy(abspath,filename,name_len);
      abspath[name_len]='\0';
      abs_len=name_len;}
    else if ((root)&&(strcmp(root,".")==0)) {
      int name_len=strlen(filename);
      abspath=malloc(name_len+1);
      strncpy(abspath,filename,name_len);
      abspath[name_len]='\0';
      abs_len=name_len;}
    else {
      char *cwd=getcwd(NULL,1024);
      int cwd_len=strlen(cwd), name_len=strlen(filename);
      if (!(cwd)) {
	fprintf(stderr,"GETCWD error %s\n",strerror(errno));
	exit(1);}
      abspath=malloc(cwd_len+name_len+2);
      strncpy(abspath,cwd,cwd_len);
      if (cwd[cwd_len-1]!='/') {
	abspath[cwd_len]='/';
	strncpy(abspath+cwd_len+1,filename,name_len);
	abs_len=cwd_len+1+name_len;
      	abspath[abs_len]='\0';}
      else {
	strncpy(abspath+cwd_len,filename,name_len);
	abs_len=cwd_len+name_len;	
	abspath[abs_len]='\0';}
      if (cwd) free(cwd);}
    relpath=abspath;
    if (root) {
      char *scan=root;
      while ((*scan)&&((scan-root)<abs_len)) {
	if (*scan==*relpath) {scan++; relpath++;}
	else break;}
      if (*relpath=='\0') relpath=abspath;}
    file_len=info.st_size;
    ptr=content=original=(unsigned char *)malloc(file_len+1);
    while (bytes_read<file_len) {
      int delta=fread(ptr,sizeof(unsigned char),BLOCK_SIZE,f);
      if (delta<0) {
	fprintf(stderr,"FREAD error %s\n",strerror(errno));
	free(content); fclose(f);
	exit(1);}
      ptr=ptr+delta; bytes_read=bytes_read+delta;}
    SHA1_Init(&context);
    SHA1_Update(&context,content,file_len);
    SHA1_Final(digest,&context);
    i=0; while (i<20) {
      int byte=digest[i];
      digest_string[2*i]=hexdigits[(byte>>4)&0xF];
      digest_string[(2*i)+1]=hexdigits[byte&0xF];
      i++;}
    digest_string[40]='\0';
    fprintf(stdout,"%s %s %d-%02d-%02dT%02d:%02d:%02d\n",
	    relpath,digest_string,
	    1900+modtime->tm_year,modtime->tm_mon+1,
	    modtime->tm_mday,
	    modtime->tm_hour,modtime->tm_min,
	    modtime->tm_sec);
    fclose(f);
    free(original);
    return 0;}
}
