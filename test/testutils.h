#pragma once

#include <hdf5.h>
#include <libgen.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK_ERR(A)                                                  \
	{                                                                 \
		if (A < 0) {                                                  \
			nerrs++;                                                  \
			printf ("Error at line %d in %s:\n", __LINE__, __FILE__); \
			goto err_out;                                             \
		}                                                             \
	}

#define EXP_ERR(A, B)                                                                           \
	{                                                                                           \
		if (A != B) {                                                                           \
			nerrs++;                                                                            \
			printf ("Error at line %d in %s: expecting %d but got %d\n", __LINE__, __FILE__, A, \
					B);                                                                         \
			goto err_out;                                                                       \
		}                                                                                       \
	}

#define EXP_VAL(V, A, T)                                                                        \
	{                                                                                           \
		if (V != A) {                                                                           \
			printf ("Error at line %d in %s: Expect " #V " = " T ", but got " T "\n", __LINE__, \
					__FILE__, A, V);                                                            \
			nerrs++;                                                                            \
		}                                                                                       \
	}

#define EXP_VAL_EX(V, A, C, T)                                                              \
	{                                                                                       \
		if (V != A) {                                                                       \
			printf ("Error at line %d in %s: Expect %s = " T ", but got " T "\n", __LINE__, \
					__FILE__, C, A, V);                                                     \
			nerrs++;                                                                        \
		}                                                                                   \
	}

#define SHOW_TEST_INFO(A)                                                                 \
	{                                                                                     \
		if (rank == 0) { printf ("*** TESTING CXX    %s: %s\n", basename (argv[0]), A); } \
	}

#define SHOW_TEST_RESULT                                                           \
	{                                                                              \
		MPI_Allreduce (MPI_IN_PLACE, &nerrs, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD); \
		if (rank == 0) {                                                           \
			if (nerrs)                                                             \
				printf ("fail with %d mismatches.\n", nerrs);                      \
			else                                                                   \
				printf ("pass\n");                                                 \
		}                                                                          \
	}

#define PASS_STR "pass\n"
#define SKIP_STR "skip\n"
#define FAIL_STR "fail with %d mismatches\n"

#define HDfprintf	 printf
#define HDfree		 free
#define failure_mssg "Fail"
#define FUNC		 "func"
#define FALSE		 0
#define TRUE		 1