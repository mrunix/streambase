////===================================================================
//
// ob_oblog_main.c liboblog / Oceanbase
//
// Copyright (C) 2013 Alipay.com, Inc.
//
// Created on 2013-05-23 by Yubai (yubai.lk@alipay.com)
//
// -------------------------------------------------------------------
//
// Description
//
//
// -------------------------------------------------------------------
//
// Change Log
//
////====================================================================

#include <stdio.h>
#include <stdlib.h>

const char my_interp[] __attribute__((section(".interp")))
  = "/lib64/ld-linux-x86-64.so.2";

const char* build_date();
const char* build_time();

int so_main() {
  fprintf(stdout, "\n");

  fprintf(stdout, "liboblog (%s %s)\n",   PACKAGE_STRING, RELEASEID);
  fprintf(stdout, "\n");

  fprintf(stdout, "BUILD_TIME: %s %s\n",  build_date(), build_time());
  exit(0);
}

void __attribute__((constructor)) ob_log_init() {
}
