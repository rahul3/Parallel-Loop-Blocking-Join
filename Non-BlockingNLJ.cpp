// parallel blocking nested loop join in offload mode

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <math.h>
#include <sys/time.h>
// dtime
// returns the current wall clock time

double dtime()
{
	double tseconds = 0.0;
	struct timeval mytime;
	gettimeofday(&mytime, (struct timezone*)0);
	tseconds = (double)(mytime.tv_sec +
                        mytime.tv_usec*1.0e-6);
	return(tseconds);
}


struct Record{
	int rid;
	int value;
	
} ;

#define TEST_MAX ((1<<30))


struct LinkedList{
       struct Node {
        int x;
        Node *next;
    };
    
public:
    // constructor
    LinkedList(){
        head = NULL; // set head to NULL
    }
    
    // This prepends a new value at the beginning of the list
   __attribute__((target(mic)))  void addValue(int val){
        Node *n = new Node();   // create new Node
        n->x = val;             // set value
        n->next = head;         // make the node point to the next node.
        if (head==NULL){
            
            _end = n;
            
        }
        //  If the list is empty, this is NULL, so the end of the list --> OK
        head = n;               // last but not least, make the head point at the new node.
    }
    
    Node* get_head(){
        return head;
    }
    
    Node* get_end(){
        return _end;
    }
    
    void linkList(LinkedList curList){
        
        Node* c_head = curList.get_head();
        Node* c_end = curList.get_end();
        _end->next = c_head;
        _end=c_end;
        
    }
    
    void print(){
        
        Node* tmp = head;
        while(tmp!=NULL){
            printf("%i \n", tmp->x);
            tmp=tmp->next;
            
        }
    }
    
    // private member
private:
    Node *head; // this is the private member variable. It is just a pointer to the first Node
    Node *_end;
};




int main(int argc, char *argv[])

{
    
    //const int counters=640000;
    const int rlen=640000;
    const int slen=640000;
    const int offset=(1<<15)-1;
    
    int result=0;
    int i=0;
    int seed=0;
    srand(seed);
    
    int startR=0;
	int endR=0;
    int data[2];
    struct Record r;
    int numResult = 0;
	int j=0;
    int k=0;
    int numthreads  , threadnum;
    double tstart, tstop, ttime;
    long totalresult=0;
    
    
    __declspec (target (mic)) struct Record* R =(struct Record*) _mm_malloc (sizeof(struct Record)  * rlen, 64);
    __declspec (target (mic)) struct Record* S =(struct Record*) _mm_malloc (sizeof(struct Record)  * slen, 64);
    
#pragma offload target (mic) inout (S) inout (R)
#pragma omp parallel
#pragma omp master
    numthreads = omp_get_num_threads();
    
      LinkedList *list = (LinkedList *)  _mm_malloc (sizeof(LinkedList)*numthreads, 64);
    
      __declspec ( target (mic)) long res[numthreads];
    printf("Initializing\n");
#pragma omp  for
    for(i=0;i<rlen;i++)
    {

        R[i].value=((((rand()& offset)<<15)+(rand()&1))+(rand()<<1)+(rand()&1))%TEST_MAX;
        R[i].rid=i;
        S[i].value=((((rand()& offset)<<15)+(rand()&1))+(rand()<<1)+(rand()&1))%TEST_MAX;
        S[i].rid=i;
    }
    for (i=0; i<numthreads; i++) {
        
        
            res[i]=0;
       
    }


    

    
	printf("Starting Compute on %d threads\n", numthreads);

	tstart = dtime();
#pragma offload target (mic) in (R : length(rlen)) in (S : length(slen)) inout(list)
#pragma omp parallel for  schedule(guided) num_threads(numthreads) private (j, k,i, startR, endR )     
  
        for(j=0;j<slen;j++)
        {
            //numResult = 0;
            threadnum= omp_get_thread_num();
            
            for(k=0;k<rlen;k++)
            {
                if ( R[k].value == S[j].value)
                {
                   res[threadnum]++;
                    numResult++;
#pragma vector aligned
                    list[threadnum].addValue(R[k].value);
                  
                    
                }
            }
        }
      
        
    }
    
    i=0;
    LinkedList fin_list = list[0];
    
    for(i=1;i<numthreads;i++){
        
        fin_list.linkList(list[i]);
    }
    
    tstop = dtime();
    
    //fin_list.print();
    
    for(k=0; k<numthreads; k++)
    {
         totalresult= totalresult + res[k];       
        
        
    }
    printf("the total result value is: %lu \n", totalresult);
        
        //elasped time
        ttime = tstop - tstart;
        
        
        if ((ttime) > 0.0)
        {
            printf("the runnung time of the algorithm in terms of Secs = %10.3lf\n", ttime);
        }
        
        _mm_free(R);
        _mm_free(S);
        _mm_free(list);
        
        
        
        
        return(0);
   
}