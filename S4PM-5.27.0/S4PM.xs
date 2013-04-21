#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"


MODULE = S4PM           PACKAGE = S4PM

int rusage(cmd, ...)
        char * cmd;
    PREINIT:
        STRLEN n_a;
    INIT:
        struct rusage ru;
        struct timeval start, stop;
        struct timezone tzp;
        double d_start, d_stop;
        int i;
        int exit_status;
        char cmd_str[1024];
        char *arg;
    CODE:
        strcpy(cmd_str, cmd);
        /* Concatenate arguments */
        for (i = 1; i < items; i++) {
            strcat(cmd_str, " ");
            arg = (char *)SvPV(ST(i), n_a);
            strcat(cmd_str, arg);
        }
        printf("Command: %s\n", cmd_str);
        gettimeofday(&start, &tzp);    
        exit_status = 0xffff & system (cmd_str);    
        gettimeofday(&stop, &tzp);    
        if (exit_status == 0xff00) {
            fprintf(stderr, "Command %s failed: %d\n", cmd, exit_status);
            /* Avoid effects from exit code wraparond at -1 and 255 */
            exit_status = 254;
        }
        /* Command executed, but exited with a non-zero status */
        else if (exit_status > 0x80) {
            exit_status = (exit_status >> 8);
            fprintf (stderr, "Job failed with exit %d\n", exit_status);
        }
        /* Command terminated abnormally (coredump or signal) */
        else if (exit_status) {
            if (exit_status & 0x80) {
                exit_status &= ~0x80;
                fprintf (stderr, "%s died with coredump from signal %d\n", cmd, exit_status & ~0x80);
            }
            else {
                fprintf (stderr, "%s died with signal %d\n", cmd, exit_status);
            }
        }
        /* Compute time difference */
        d_start = start.tv_usec / 1000000.;
        d_stop = (stop.tv_sec - start.tv_sec) + stop.tv_usec / 1000000.;
        getrusage(-1, &ru);
        printf("COMMAND=%s\n", cmd);
        printf("EXIT_STATUS=%d\n", exit_status);
        printf("ELAPSED_TIME=%lf\n", d_stop - d_start);
        printf("USER_TIME=%ld.%d\n", ru.ru_utime.tv_sec, ru.ru_utime.tv_usec);
        printf("SYSTEM_TIME=%ld.%d\n", ru.ru_stime.tv_sec, ru.ru_stime.tv_usec);
        printf("MAXIMUM_RESIDENT_SET_SIZE=%ld\n", ru.ru_maxrss);
        printf("AVERAGE_SHARED_TEXT_SIZE=%ld\n", ru.ru_ixrss);
        printf("AVERAGE_UNSHARED_DATA_SIZE=%ld\n", ru.ru_idrss);
        printf("AVERAGE_UNSHARED_STACK_SIZE=%ld\n", ru.ru_isrss);
        printf("PAGE_RECLAIMS=%ld\n", ru.ru_minflt);
        printf("PAGE_FAULTS=%ld\n", ru.ru_majflt);
        printf("SWAPS=%ld\n", ru.ru_nswap);
        printf("BLOCK_INPUT_OPERATIONS=%ld\n", ru.ru_inblock);
        printf("BLOCK_OUTPUT_OPERATIONS=%ld\n", ru.ru_oublock);
        printf("MESSAGES_SENT=%ld\n", ru.ru_msgsnd);
        printf("MESSAGES_RECEIVED=%ld\n", ru.ru_msgrcv);
        printf("SIGNALS_RECEIVED=%ld\n", ru.ru_nsignals);
        printf("VOLUNTARY_CONTEXT_SWITCHES=%ld\n", ru.ru_nvcsw);
        printf("INVOLUNTARY_CONTEXT_SWITCHES=%ld\n", ru.ru_nivcsw);
        fflush(stdout);
        RETVAL = exit_status;
    OUTPUT:
        RETVAL

