//NOTE: THIS CODE IS BASED OFF WHAT WE DISCUSSED IN CLASS. ATTRIBUTION, NAT TUCK, CLASS. 
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include <time.h>
#include <float.h>
#include <semaphore.h>
#include <pthread.h>
#include <string.h>

#include "float_vec.h"
#include "barrier.h"
#include "utils.h"

typedef struct sortArgs {
	int pnum;
	float* data;	
	long size;
	int P;
	floats* samps;
	long* sizes;
	barrier* bb;
	const char* file; 
	int fileSize; 
} sortArgs;

int
comparing(const void* first, const void* second) {
	float one = *((float*)first);
	float two = *((float*)second);
	return (one > two) - (one < two); 
}

void
qsort_floats(floats* xs)
{
    // call qsort to sort the array
	qsort(xs->data, xs->size, sizeof(float), comparing); 
}

floats*
sample(float* data, long size, int P)
{
    // sample the input data, per the algorithm description
    int numSort = 3 * (P-1);

    srand(time(0));

    floats* randomFloats = make_floats(numSort);
    for (int i = 0; i < numSort; i++) {
	int randyNum = rand() % size; 
	floats_push(randomFloats, *(data + randyNum)); 
    }

    qsort_floats(randomFloats);

    floats* sampleFloats = make_floats(P-1);
    for (int i = 0; i < (P-1); i++) {
	    float firstFloat = randomFloats->data[(i*3)];
	    float secondFloat = randomFloats->data[(i*3)+1];
	    float thirdFloat = randomFloats->data[(i*3)+2];
	    float median = (firstFloat + secondFloat + thirdFloat) / 3;
	    floats_push(sampleFloats, median);
    } 

    floats* finalFloats = make_floats(P-1);
    floats_push(finalFloats, 0);
    for (int i = 0; i < sampleFloats->size; i++) {
	   floats_push(finalFloats, sampleFloats->data[i]);
    }

    floats_push(finalFloats, FLT_MAX);
    free_floats(randomFloats);
    free_floats(sampleFloats); 

    //floats_print(finalFloats);  
 
    return finalFloats;
}

void*
sort_worker(void* arg)
{

    sortArgs args = *((sortArgs*) arg); 

    floats* xs = make_floats(0);
    
    // select the floats to be sorted by this worker
    for (long i = 0; i < args.size; i++) {
	float nextNum = args.data[i];
	if (nextNum >= args.samps->data[args.pnum] && nextNum < args.samps->data[args.pnum+1]) {
		floats_push(xs, nextNum);
	}
    }

    printf("%d: start %.04f, count %ld\n", args.pnum, args.samps->data[args.pnum], xs->size);

    //write number of items to shared array at pnum
    args.sizes[args.pnum] = xs->size;
    //printf("Size p is %li\n", sizes[pnum]); 

    qsort_floats(xs);
    //printf("Sorted floats is ");
    //floats_print(xs); 

    //copy local arrays -- need to avoid data race here
    barrier_wait(args.bb); 
    
    int start = 0;
    for (int i = 0; i <= args.pnum-1; i++) {
	start += args.sizes[i];
    }

    int end = 0;
    for (int i = 0; i <= args.pnum; i++) {
	    end += args.sizes[i];
    }
    end--; 

    //printf("Start value is %d for process %d\n", start, args.pnum);
    //printf("End value is %d for process %d\n", end, args.pnum); 

    //barrier_wait(args.bb); 
    
    if (start == 0) {
	    int fl = open(args.file, O_CREAT | O_WRONLY | O_APPEND, 0777);
	    long fileSize = args.size; 
	    write(fl, &fileSize, sizeof(long)); 
	    close(fl);  
    }

    int fo = open(args.file, O_CREAT | O_WRONLY | O_APPEND, 0777);

    int j = 0;
    for (int i = start; i <= end; i++) {
	//args.data[i] = xs->data[j];
	float message = xs->data[j];
	lseek(fo, i+1, SEEK_SET); 
	write(fo, &message, sizeof(float)); 
	j++;
    }

    close(fo); 

    free_floats(xs);
    pthread_exit(0); 
}

void
run_sort_workers(float* data, long size, int P, floats* samps, long* sizes, barrier* bb, const char* file, int fileSize)
{
    pthread_t threads[P];

    sortArgs* job = malloc(sizeof(sortArgs));
    job->data = data;
    job->size = size;
    job->P = P;
    job->samps = samps;
    job->sizes = sizes;
    job->bb = bb;
    job->file = file;
    job->fileSize = fileSize;  


    // spawn P threads, each running sort_worker
    for (int i = 0; i < P; i++) {
	    job->pnum = i; 
	    int rv = pthread_create(&(threads[i]), 0, &sort_worker, job);
	    check_rv(rv); 
    }

    for (int ii = 0; ii < P; ++ii) {
        int rv = pthread_join(threads[ii], 0);
        check_rv(rv);
    }

    free(job); 

}

void
sample_sort(float* data, long size, int P, long* sizes, barrier* bb, const char* file, int fileSize)
{
    floats* samps = sample(data, size, P);
    run_sort_workers(data, size, P, samps, sizes, bb, file, fileSize);
    free_floats(samps);
}

int
main(int argc, char* argv[])
{
    alarm(120);

    if (argc != 4) {
        printf("Usage:\n");
        printf("\t%s P data.dat output.dat\n", argv[0]);
        return 1;
    }

    const int P = atoi(argv[1]);
    const char* inFname = argv[2];
    //printf("file name is %s\n", inFname); 
    const char* outFname = argv[3];

    seed_rng();

    int rv;
    struct stat st;
    rv = stat(inFname, &st);
    check_rv(rv);

    const int fsize = st.st_size;
    if (fsize < 8) {
        printf("File too small.\n");
        return 1;
    }

    FILE* fd = fopen(inFname, "rb");

    floats* my_floats = make_floats(0);

    long count;

    fread(&count, sizeof(long), 1, fd); 
    //printf("The count is %ld\n", count); 
    
    float data; 
    for (int i = 0; i < count; i++) {
	    fread(&data, sizeof(float), 1, fd);
	    floats_push(my_floats, data); 
    }



    /*for (int i = 0; i < count; i++) {
	    printf("Data at %d is %f\n", i, my_floats->data[i]);
    }*/

    long* sizes = calloc(P, sizeof(long));

    int fileSize = sizeof(fd); 

    fclose(fd);  


    barrier* bb = make_barrier(P);

    sample_sort(my_floats->data, count, P, sizes, bb, outFname, fileSize);

    /*for (int i = 0; i < count; i++) {
	    printf("Data[%d] is %f\n", i, my_floats->data[i]);
    }*/


    free_barrier(bb);
    free(sizes);
    //free(my_floats); 

    return 0;
}

