#ifndef __BASE__H_
#define __BASE__H_

int Base64decode_len(const char *bufcoded);
int Base64decode(char *bufplain, const char *bufcoded);
int Base64encode_len(int len);
int Base64encode(char *encoded, const char *string, int len);

#endif
