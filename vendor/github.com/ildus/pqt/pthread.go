// +build linux

package pqt

/*
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
extern void createThreadCallback();
static void sig_func(int sig);
static void createThread(pthread_t* pid) {
	pthread_create(pid, NULL, (void*)createThreadCallback, NULL);
}
static void sig_func(int sig)
{
	fprintf(stderr, "thread killed\n");
	pthread_exit(NULL);
}
static void register_sig_handler() {
	signal(SIGTERM, sig_func);
}
*/
import "C"
import "unsafe"

type Pthread uintptr
type ThreadCallback func()

var create_callback chan ThreadCallback

func init() {
	C.register_sig_handler()
	create_callback = make(chan ThreadCallback, 1)
}

//export createThreadCallback
func createThreadCallback() {
	C.register_sig_handler()
	C.pthread_setcanceltype(C.PTHREAD_CANCEL_ASYNCHRONOUS, nil)
	(<-create_callback)()
	C.pthread_exit(unsafe.Pointer(C.NULL))
}

// calls C's sleep function
func Sleep(seconds uint) {
	C.sleep(C.uint(seconds))
}

// initializes a thread using pthread_create
func makeThread(cb ThreadCallback) Pthread {
	var pid C.pthread_t
	pidptr := &pid
	create_callback <- cb

	C.createThread(pidptr)

	return Pthread(uintptr(unsafe.Pointer(&pid)))
}

// signals the thread in question to terminate
func (t Pthread) Kill() {
	C.pthread_kill(t.c(), C.SIGTERM)
}

// helper function to convert the Pthread object into a C.pthread_t object
func (t Pthread) c() C.pthread_t {
	return *(*C.pthread_t)(unsafe.Pointer(t))
}
