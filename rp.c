/* assert */
#include <assert.h>

/* fabs */
#include <math.h>

/* MPI API */
#include <mpi.h>

/* printf, fopen, fclose, fscanf, scanf */
#include <stdio.h>

/* EXIT_SUCCESS, malloc, calloc, free, qsort */
#include <stdlib.h>

#define MPI_SIZE_T MPI_UNSIGNED_LONG

struct distance_metric {
  size_t viewer_id;
  double distance;
};

static int
cmp(void const *ap, void const *bp)
{
  struct distance_metric const a = *(struct distance_metric*)ap;
  struct distance_metric const b = *(struct distance_metric*)bp;

  return a.distance < b.distance ? -1 : 1;
}

int
main(int argc, char * argv[])
{
  int ret, p, rank;
  size_t n, m, k;
  double * rating;
  /* Compute base number of viewers. */



  /* Initialize MPI environment. */
  ret = MPI_Init(&argc, &argv);
  assert(MPI_SUCCESS == ret);

  /* Get size of world communicator. */
  ret = MPI_Comm_size(MPI_COMM_WORLD, &p);
  assert(ret == MPI_SUCCESS);

  /* Get my rank. */
  ret = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  assert(ret == MPI_SUCCESS);

  /* Validate command line arguments. */
  assert(2 == argc);
///////////////////////////////////////////////////////////////////////////////
  /* Read input --- only if your rank 0. */
 if(rank == 0)
 {
      char const * const fn = argv[1];

      /* Validate input. */
      assert(fn);

      /* Open file. */
      FILE * const fp = fopen(fn, "r");
      assert(fp);

      /* Read file. */
      fscanf(fp, "%zu %zu", &n, &m);

      /* Allocate memory. */
      rating = malloc(n * m * sizeof(*rating));
      assert(rating);

      for (size_t i = 0; i < n; i++) {
        for (size_t j = 0; j < m; j++) {
          fscanf(fp, "%lf", &rating[i * m + j]);
        }
      }

      /* Close file. */
      ret = fclose(fp);
      assert(!ret);
  }
////////////////////////////////////////////////////////////////////////////////

 if (rank == 0) {

     /* Send number of viewers and movies to rest of processes. */
     for (int r = 1; r < p; r++)
     {
       ret = MPI_Send(&n, 1, MPI_SIZE_T, r, 0, MPI_COMM_WORLD);
       assert(MPI_SUCCESS == ret);
       ret = MPI_Send(&m, 1, MPI_SIZE_T, r, 0, MPI_COMM_WORLD);
       assert(MPI_SUCCESS == ret);
     }
  }
 else {
     ret = MPI_Recv(&n, 1, MPI_SIZE_T, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
     assert(MPI_SUCCESS == ret);
     ret = MPI_Recv(&m, 1, MPI_SIZE_T, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
     assert(MPI_SUCCESS == ret);
 }
///////////////////////////////////////////////////////////////////////////

  /* Base number of viewers using cieling calculation */
  size_t const base = 1 + ((n - 1) / p); // ceil(n / p)

  size_t const nrows = (rank + 1) * base > n ? n - rank * base : base;

  if(rank == 0) {
      for (int i = 1; i < p; i++)
      {
        size_t const rnrows = (i + 1) * base > n ? n - i * base : base;
        ret = MPI_Send(&(rating[i * base * m]), rnrows * m, MPI_DOUBLE, i, 0, MPI_COMM_WORLD);
        assert(MPI_SUCCESS == ret);
      }
  }

  else {
    /* Allocating Memory & checking with assert */
    rating = malloc(nrows * m * sizeof(*rating));
    assert(rating);
    ret = MPI_Recv(rating, nrows * m, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    assert(MPI_SUCCESS == ret);
  }

/////////////////////////////////////////////////////////////////////////

/* Printing out all the ratings and which rank is reciveing them */
for (int t = 0; t < nrows; t++)
{
  for (int c = 0; c < m -1; c++)
  {
    printf("Process: %d reciving: %lf\n", rank, rating[t * m + c]);
    fflush(stdout);
  }
}

////////////////////////////////////////////////////////////////////////////

  /* Allocating Memory for user Rating & testing with asseert */
  double * const urating = malloc(m * sizeof(*urating));
  assert(urating);

  if(rank == 0){
    for (size_t j = 0; j < m - 1; j++) {
      printf("Enter your rating for movie %zu: \n", j + 1);
      fflush(stdout);
      scanf("%lf", &urating[j]);
    }
    for (size_t z = 1; z < m - 1; z++)
    {
      ret = MPI_Send(urating, m - 1, MPI_DOUBLE, z, 0, MPI_COMM_WORLD);
      assert(MPI_SUCCESS == ret);
    }
  }

  else {
    ret = MPI_Recv(urating, m - 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    assert(MPI_SUCCESS == ret);
  }

//////////////////////////////////////////////////////////////////////////

  /* Allocate more memory. & Test with assert */
  double * const distance = calloc(n, sizeof(*distance));
  assert(distance);

  if (rank != 0)
  {
    double * const distance = calloc(n, sizeof(*distance));
    assert(distance);
    /* Compute distances. */
    for (size_t i = 0; i < nrows; i++) {
      for (size_t j = 0; j < m - 1; j++) {
        distance[i] += fabs(urating[j] - rating[i * m + j]);
      }
    }
    ret = MPI_Send(distance, nrows, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    assert(MPI_SUCCESS == ret);
  }

  if (0 == rank)
  {
    double * const distance = calloc(n, sizeof(*distance));
    assert(distance);
    /* Compute distances. */
    for (size_t i = 0; i < nrows; i++) {
      for (size_t j = 0; j < m - 1; j++) {
        distance[i] += fabs(urating[j] - rating[i * m + j]);
      }
    }



    for (int i = 1; i < p; i++)
    {
      ret = MPI_Recv(distance + i * nrows, nrows, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      assert(ret == MPI_SUCCESS);
    }

    struct distance_metric * matrix = malloc(n * sizeof(*matrix));
    for (int i = 0; i < n; i ++){
      matrix[i].viewer_id = i;
      matrix[i].distance = distance[i];
    }

    qsort(matrix, n, sizeof(*matrix), cmp);

    /* Get user input. */
   printf("Enter the number of similar viewers to report: ");
   fflush(stdout);
   scanf("%zu", &k);

   /* Output k viewers who are least different from the user. */
   printf("Viewer ID   Movie five   Distance\n");
   fflush(stdout);

   printf("---------------------------------\n");
   fflush(stdout);


   for (size_t i = 0; i < k; i++) {
     printf("%9zu   %10.1lf   %8.1lf\n", matrix[i].viewer_id + 1,
 rating[matrix[i].viewer_id * m + 4], matrix[i].distance);
   }

   printf("---------------------------------\n");
   fflush(stdout);


   /* Compute the average to make the prediction. */
   double sum = 0.0;
   for (size_t i = 0; i < k; i++) {
     sum += rating[matrix[i].viewer_id * m + 4];
   }

   /* Output prediction. */
   printf("The predicted rating for movie five is %.1lf.\n", sum / k);
   printf("\n");
   fflush(stdout);

 }








///////////////////////////////////////////////////////////////////////////




























  /* Deallocate memory. */
  free(rating);
  fflush(stdout);
  free(urating);
  fflush(stdout);
  free(distance);
  fflush(stdout);

  ret = MPI_Finalize();
  assert(MPI_SUCCESS == ret);

  return EXIT_SUCCESS;

  }
