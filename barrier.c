// Author: Nat Tuck
// CS3650 starter code

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>

#include "barrier.h"

barrier*
make_barrier(int nn)
{
    barrier* bb = malloc(sizeof(barrier));
    assert(bb != 0);
    pthread_mutex_init(&bb->mute, NULL);
    pthread_cond_init(&bb->condv, NULL); 
    bb->count = nn;
    bb->seen  = 0;
    return bb;
}

void
barrier_wait(barrier* bb)
{

	pthread_mutex_lock(&bb->mute);
	bb->seen++;
	if (bb->seen == bb->count) {
		pthread_cond_broadcast(&bb->condv);
	}
	else {
		while (bb->seen != bb->count) {
			pthread_cond_wait(&bb->condv, &bb->mute);
		}
	}

	pthread_mutex_unlock(&bb->mute); 
    
}

void
free_barrier(barrier* bb)
{
    free(bb);
}

