/*
 * This code is provided solely for the personal and private use of students
 * taking the CSC369H course at the University of Toronto. Copying for purposes
 * other than this use is expressly prohibited. All forms of distribution of
 * this code, including but not limited to public repositories on GitHub,
 * GitLab, Bitbucket, or any other online platform, whether as given or with
 * any changes, are expressly prohibited.
 *
 * Authors: Alexey Khrabrov, Andrew Pelegris, Karen Reid
 *
 * All of the files in this directory and all subdirectories are:
 * Copyright (c) 2019, 2020 Karen Reid
 */

/**
 * CSC369 Assignment 2 - Message queue implementation.
 *
 * You may not use the pthread library directly. Instead you must use the
 * functions and types available in sync.h.
 */

#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include "errors.h"
#include "list.h"
#include "msg_queue.h"
#include "ring_buffer.h"


// Message queue implementation backend
typedef struct mq_backend {
	// Ring buffer for storing the messages
	ring_buffer buffer;

	// Reference count
	size_t refs;

	// Number of handles open for reads
	size_t readers;
	// Number of handles open for writes
	size_t writers;

	// Set to true when all the reader handles have been closed. Starts false
	// when they haven't been opened yet.
	bool no_readers;
	// Set to true when all the writer handles have been closed. Starts false
	// when they haven't been opened yet.
	bool no_writers;

	// 	 add necessary synchronization primitives, as well as data structures
	//      needed to implement the msg_queue_poll() functionality
	
	mutex_t mq_mutex;
	int event_flag;
	list_head wait_q;
	cond_t is_empty;
	cond_t is_full;
	
} mq_backend;


static int mq_init(mq_backend *mq, size_t capacity)
{
	if (ring_buffer_init(&mq->buffer, capacity) < 0) {
		return -1;
	}

	mq->refs = 0;

	mq->readers = 0;
	mq->writers = 0;

	mq->no_readers = false;
	mq->no_writers = false;

	// initialize remaining fields (synchronization primitives, etc.)
	mutex_init(&mq->mq_mutex);
	list_init(&mq->wait_q);
	cond_init(&mq->is_empty);
	cond_init(&mq->is_full);

	return 0;
}

static void mq_destroy(mq_backend *mq)
{
	assert(mq->refs == 0);
	assert(mq->readers == 0);
	assert(mq->writers == 0);

	ring_buffer_destroy(&mq->buffer);

	// cleanup remaining fields (synchronization primitives, etc.)
	mutex_destroy(&mq->mq_mutex);
	list_destroy(&mq->wait_q);
	cond_destroy(&mq->is_empty);
	cond_destroy(&mq->is_full);
}


#define ALL_FLAGS (MSG_QUEUE_READER | MSG_QUEUE_WRITER | MSG_QUEUE_NONBLOCK)


// Message queue handle is a combination of the pointer to the queue backend and
// the handle flags. The pointer is always aligned on 8 bytes - its 3 least
// significant bits are always 0. This allows us to store the flags within the
// same word-sized value as the pointer by ORing the pointer with the flag bits.

// Get queue backend pointer from the queue handle
static mq_backend *get_backend(msg_queue_t queue)
{
	mq_backend *mq = (mq_backend*)(queue & ~ALL_FLAGS);
	assert(mq);
	return mq;
}

// Get handle flags from the queue handle
static int get_flags(msg_queue_t queue)
{
	return (int)(queue & ALL_FLAGS);
}

// Create a queue handle for given backend pointer and handle flags
static msg_queue_t make_handle(mq_backend *mq, int flags)
{
	assert(((uintptr_t)mq & ALL_FLAGS) == 0);
	assert((flags & ~ALL_FLAGS) == 0);
	return (uintptr_t)mq | flags;
}


static msg_queue_t mq_open(mq_backend *mq, int flags)
{
	++mq->refs;

	if (flags & MSG_QUEUE_READER) {
		++mq->readers;
		mq->no_readers = false;
	}
	if (flags & MSG_QUEUE_WRITER) {
		++mq->writers;
		mq->no_writers = false;
	}

	return make_handle(mq, flags);
}

// Returns true if this was the last handle
static bool mq_close(mq_backend *mq, int flags)
{
	assert(mq->refs != 0);
	assert(mq->refs >= mq->readers);
	assert(mq->refs >= mq->writers);

	if ((flags & MSG_QUEUE_READER) && (--mq->readers == 0)) {
		mq->no_readers = true;
	}
	if ((flags & MSG_QUEUE_WRITER) && (--mq->writers == 0)) {
		mq->no_writers = true;
	}

	if (--mq->refs == 0) {
		assert(mq->readers == 0);
		assert(mq->writers == 0);
		return true;
	}
	return false;
}


msg_queue_t msg_queue_create(size_t capacity, int flags)
{
	if (flags & ~ALL_FLAGS) {
		errno = EINVAL;
		report_error("msg_queue_create");
		return MSG_QUEUE_NULL;
	}

	// Refuse to create a message queue without capacity for
	// at least one message (length + 1 byte of message data).
	if (capacity < sizeof(size_t) + 1) {
		errno = EINVAL;
		report_error("msg_queue_create");
		return MSG_QUEUE_NULL;
	}

	mq_backend *mq = (mq_backend*)malloc(sizeof(mq_backend));
	if (!mq) {
		report_error("malloc");
		return MSG_QUEUE_NULL;
	}
	// Result of malloc() is always aligned on 8 bytes, allowing us to use the
	// 3 least significant bits of the handle to store the 3 bits of flags
	assert(((uintptr_t)mq & ALL_FLAGS) == 0);

	if (mq_init(mq, capacity) < 0) {
		// Preserve errno value that can be changed by free()
		int e = errno;
		free(mq);
		errno = e;
		return MSG_QUEUE_NULL;
	}

	return mq_open(mq, flags);
}

msg_queue_t msg_queue_open(msg_queue_t queue, int flags)
{
	if (!queue) {
		errno = EBADF;
		report_error("msg_queue_open");
		return MSG_QUEUE_NULL;
	}

	if (flags & ~ALL_FLAGS) {
		errno = EINVAL;
		report_error("msg_queue_open");
		return MSG_QUEUE_NULL;
	}

	mq_backend *mq = get_backend(queue);

	// add necessary synchronization

	mutex_lock(&mq->mq_mutex);
	msg_queue_t handle;
	handle = mq_open(mq, flags);
	mutex_unlock(&mq->mq_mutex);
	return handle;
}

int msg_queue_close(msg_queue_t *queue)
{
	if (!queue || !*queue) {
		errno = EBADF;
		report_error("msg_queue_close");
		return -1;
	}

	mq_backend *mq = get_backend(*queue);
	
	// add necessary synchronization
	mutex_lock(&mq->mq_mutex);
	if (mq_close(mq, get_flags(*queue))) {
		// Closed last handle; destroy the queue
		*queue = MSG_QUEUE_NULL;
		mutex_unlock(&mq->mq_mutex);
		mq_destroy(mq);
		return 0;
	}

	//	if this is the last reader (or writer) handle, notify all the writer
	//      (or reader) threads currently blocked in msg_queue_write() (or
	//      msg_queue_read()) and msg_queue_poll() calls for this queue.
	int q_no_readers = mq->no_readers;
	int q_no_writers = mq->no_writers;
	if(q_no_writers && q_no_writers){
		mq->event_flag = mq->event_flag | MQPOLL_READABLE | MQPOLL_NOWRITERS;
		cond_broadcast(&mq->is_empty); 
		mq->event_flag = mq->event_flag | MQPOLL_WRITABLE | MQPOLL_NOREADERS;
		cond_broadcast(&mq->is_full); 
	}
	else if(q_no_readers){
		mq->event_flag = mq->event_flag | MQPOLL_WRITABLE | MQPOLL_NOREADERS;
		cond_broadcast(&mq->is_full); 
	}
	else if(q_no_writers){
		mq->event_flag = mq->event_flag | MQPOLL_READABLE | MQPOLL_NOWRITERS;
		cond_broadcast(&mq->is_empty); 
	}
	*queue = MSG_QUEUE_NULL;
	mutex_unlock(&mq->mq_mutex);
	return 0;
}

typedef struct thread_in_wait{
	cond_t poll_ready;
	mutex_t poll_mutex;
} thread_in_wait;

typedef struct thread_in_wait_entry{
	list_entry lentry;
	list_head lhead;
	int thread_event;
	thread_in_wait *thread;
} thread_in_wait_entry;

ssize_t msg_queue_read(msg_queue_t queue, void *buffer, size_t length)
{
	int queue_flags = get_flags(queue);
	// queue is not a valid message queue handle open for reads.
	if (!queue_flags & !MSG_QUEUE_READER) {
		errno = EBADF;
		report_error("msg_queue_read");
		return -1;
	}
	size_t size;
	mq_backend *mq = get_backend(queue);
	mutex_lock(&mq->mq_mutex); 
	int q_is_empty = ring_buffer_used(&mq->buffer) == 0;
	while (q_is_empty) {
		// the queue is empty and all the writer handles to the queue 
		// have been closed ("end of file")
		if (mq->no_writers) {
			mq->event_flag = mq->event_flag | MQPOLL_NOWRITERS;
			// list to store the cond_variable
			// declare variable to point to head of the list
			list_head *a_list;
			a_list = &mq->wait_q;
			list_entry *current;
			struct thread_in_wait_entry *wait_threads;
			list_for_each(current, a_list) {
				wait_threads = container_of(current, struct thread_in_wait_entry, lentry);
				mutex_lock(&wait_threads->thread->poll_mutex);
				if(wait_threads->thread_event &mq->event_flag){ 
					cond_signal(&wait_threads->thread->poll_ready); 
				}
				mutex_unlock(&wait_threads->thread->poll_mutex);
			}
			mutex_unlock(&mq->mq_mutex);
			return 0;
		}
		// The queue handle is non-blocking and the read would block
		// because there is no message in the queue to read.
		if (MSG_QUEUE_NONBLOCK & queue_flags) {
			errno = EAGAIN;
			report_info("msg_queue_read");
			mutex_unlock(&mq->mq_mutex); 
			return -1;
		}
		cond_wait(&mq->is_empty, &mq->mq_mutex);
		q_is_empty = ring_buffer_used(&mq->buffer) == 0;
	}
	
	if (ring_buffer_peek(&mq->buffer, &size, sizeof(size_t))) {
		// The buffer is not large enough to hold the message.
		if (size > length) {
			errno = EMSGSIZE;
			report_info("msg_queue_read");
			mutex_unlock(&mq->mq_mutex); 
			return ~size;
		}
		// read the size and the size bytes from the message queue
		ring_buffer_read(&mq->buffer, &size, sizeof(size_t));
		ring_buffer_read(&mq->buffer, buffer, size);
		q_is_empty = ring_buffer_used(&mq->buffer) == 0;
		cond_signal(&mq -> is_full);
		// if queue is empty and has writers
		if (q_is_empty && !(mq->no_writers)) {
			// change all bits to 0
			mq->event_flag = mq->event_flag & 0;
		}
		mq->event_flag = mq->event_flag | MQPOLL_WRITABLE;
		// list to store the cond_variable
		list_head *a_list;
		a_list = &mq->wait_q;
		list_entry *current;
		struct thread_in_wait_entry *wait_threads;
		list_for_each(current, a_list) {
			wait_threads = container_of(current, struct thread_in_wait_entry, lentry);
			mutex_lock(&wait_threads->thread->poll_mutex);
			if(wait_threads->thread_event &mq->event_flag){ 
				cond_signal(&wait_threads->thread->poll_ready); 
			}
			mutex_unlock(&wait_threads->thread->poll_mutex);
		}
		mutex_unlock(&mq->mq_mutex); 
		return size;
	}
	mutex_unlock(&mq->mq_mutex); 
	return -1;
}

int msg_queue_write(msg_queue_t queue, const void *buffer, size_t length)
{
	// handle error
	if(length == 0 || !(get_flags(queue) & MSG_QUEUE_WRITER)){
		if (length == 0) {
			//zero length message
			errno = EINVAL;
		} else if (!(get_flags(queue) & MSG_QUEUE_WRITER)){
			//queue is not a valid message queue handle open for writes.
			errno = EBADF;
		}
		report_error("msg_queue_write");
		return -1;
	}
	mq_backend *mq = get_backend(queue);
	mutex_lock(&mq->mq_mutex); 
	size_t msg_size = length + sizeof(size_t);
	// left_over space to write
	size_t free_space = ring_buffer_free(&mq->buffer);
	while(free_space < msg_size){
		//check capacity of queue
		if(msg_size > (mq->buffer.size)){
			errno = EMSGSIZE;
			report_info("msg_queue_write");
			mutex_unlock(&mq->mq_mutex); 
			return -1;
		}
		else if(mq->no_readers){
			int new_flag = mq->event_flag | MQPOLL_NOREADERS;
			mq->event_flag = new_flag;
			list_entry *current;
			list_head *a_list = &mq->wait_q;
			//get thread from poll for writing
			list_for_each(current, a_list){
				struct thread_in_wait_entry *wait_threads;
				wait_threads = container_of(current, struct thread_in_wait_entry, lentry);
				mutex_lock(&wait_threads->thread->poll_mutex);
				if(wait_threads->thread_event &mq->event_flag){ 
					cond_signal(&wait_threads->thread->poll_ready); 
				}
				mutex_unlock(&wait_threads->thread->poll_mutex);
			}
			errno = EPIPE;
			report_info("msg_queue_write");
			mutex_unlock(&mq->mq_mutex); 
			return -1;
		}
		else if (!mq->no_readers){
			if(get_flags(queue) & MSG_QUEUE_NONBLOCK){
				errno = EAGAIN;
				report_info("msg_queue_write");
				mutex_unlock(&mq->mq_mutex); 
				return -1;
			}
			cond_wait(&mq->is_full, &mq->mq_mutex);
			free_space = ring_buffer_free(&mq->buffer);
		}
	}
	// all reader handles to the queue have been closed ("broken pipe").
	ring_buffer_write(&mq->buffer, &length, sizeof(size_t));
	ring_buffer_write(&mq->buffer, buffer, length);
	cond_signal(&mq->is_empty);
	int new_flag = mq->event_flag | MQPOLL_READABLE;
	mq->event_flag = new_flag;
	// no more free space but still readers
	if(free_space == 0){
		if (!mq->no_readers){
			int new_event_flag = mq->event_flag & ~MQPOLL_WRITABLE;
			mq->event_flag = new_event_flag;
		}
	}
	list_entry *current;
	list_head *a_list = &mq->wait_q;
	//get thread from poll for writing
	list_for_each(current, a_list){
		struct thread_in_wait_entry *wait_threads;
		wait_threads = container_of(current, struct thread_in_wait_entry, lentry);
		mutex_lock(&wait_threads->thread->poll_mutex);
		if(wait_threads->thread_event &mq->event_flag){ 
			cond_signal(&wait_threads->thread->poll_ready); 
		}
		mutex_unlock(&wait_threads->thread->poll_mutex);
	}
	mutex_unlock(&mq->mq_mutex); 
	return 0;
}

int msg_queue_poll(msg_queue_pollfd *fds, size_t nfds)
{
	// No events are subscribed to
	if (nfds == 0) {
		errno = EINVAL;
		report_error("msg_queue_poll");
		return -1;
	}
	int count = 0; // count for the number of MSG_QUEUE_NULL
	for (size_t i = 0; i < nfds; ++i) {
		fds[i].revents = 0;
		int queue_flags = get_flags(fds[i].queue);
		if (fds[i].queue != MSG_QUEUE_NULL) {
			// handle errors
			int is_readable = fds[i].events & MQPOLL_READABLE;
			int is_writable = fds[i].events & MQPOLL_WRITABLE;
			// MQPOLL_READABLE requested for a non-reader queue handle
			if (is_readable && !(queue_flags & MSG_QUEUE_READER)) {
				errno = EINVAL;
				report_error("msg_queue_poll");
				return -1;
			}
           	// MQPOLL_WRITABLE requested for a non-writer queue handle
  			if (is_writable && !(queue_flags & MSG_QUEUE_WRITER)) {
				errno = EINVAL;
				report_error("msg_queue_poll");
				return -1;
			}
			// events field in a pollfd entry is invalid (no events)
			if (fds[i].events & ~(MQPOLL_NOWRITERS | MQPOLL_NOREADERS | MQPOLL_READABLE | MQPOLL_WRITABLE)) {
				errno = EINVAL;
				report_error("msg_queue_poll");
				return -1;
			}
		} else {
			count++;
		}
		
	}
	// all the pollfd entries have the queue field set to MSG_QUEUE_NULL
	if ((int)nfds == count) {
		errno = EINVAL;
		report_error("msg_queue_poll");
		return -1;
	}

	int thread_size = sizeof(thread_in_wait);
	int entry_size = sizeof(thread_in_wait_entry);
	struct thread_in_wait *wait_thread;
	struct thread_in_wait_entry *wait_threads_entry;
	wait_thread = (struct thread_in_wait *) malloc(thread_size);
	wait_threads_entry = (struct thread_in_wait_entry *) malloc(nfds * entry_size);
	mutex_init(&wait_thread->poll_mutex);
	cond_init(&wait_thread->poll_ready);
	mutex_lock(&wait_thread->poll_mutex);

	mq_backend *mq;
	// put threads to wait thread entry
	for(size_t i = 0; i < nfds; ++i){
		if(fds[i].queue != MSG_QUEUE_NULL){
			mq = get_backend(fds[i].queue);
			list_head *a_list = &mq->wait_q;
			wait_threads_entry[i].thread = wait_thread;
			wait_threads_entry[i].thread_event = fds[i].events;
			mutex_lock(&mq->mq_mutex);
			list_entry_init(&wait_threads_entry[i].lentry);
			list_add_tail(a_list, &wait_threads_entry[i].lentry);
			mutex_unlock(&mq->mq_mutex);
		}
	}

	int count_poll_ready = 0;
	for (size_t i = 0; i < nfds; ++i) {
		int q_flags = get_flags(fds[i].queue);
		if (fds[i].queue != MSG_QUEUE_NULL) {
			mq = get_backend(fds[i].queue);
			fds[i].revents = mq->event_flag & fds[i].events;
			if (fds[i].revents > 0) {
				count_poll_ready++;
			}
			// If the queue handle is a reader or writer handle,
			// MQPOLL_NOWRITERS or MQPOLL_NOREADERS is reported
			if ((MSG_QUEUE_READER | MSG_QUEUE_WRITER) & q_flags) {
				int new_event = (MQPOLL_NOWRITERS | MQPOLL_NOREADERS) & q_flags;
				fds[i].revents = fds[i].revents | new_event;
			}
		}
	}
	if(count_poll_ready != 0){
		mutex_unlock(&wait_thread->poll_mutex);
		//remove thread from queue to do actions
		for (size_t i = 0; i < nfds; ++i){
			if(fds[i].queue != MSG_QUEUE_NULL){
				mq = get_backend(fds[i].queue);
				list_head *a_list = &mq->wait_q;
				mutex_lock(&mq->mq_mutex);	
				list_del(a_list, &wait_threads_entry[i].lentry);
				mutex_unlock(&mq->mq_mutex);
			}
		}
		cond_destroy(&wait_thread->poll_ready);
		//free the memory
		free(wait_thread);
		free(wait_threads_entry);
		// After the blocking wait is complete, determine what events have been triggered, 
		// and return the number of message queues with any triggered events.
		return count_poll_ready;
	}
	cond_wait(&wait_thread->poll_ready, &wait_thread->poll_mutex);
	for (size_t i = 0; i < nfds; ++i) {
		int q_flags = get_flags(fds[i].queue);
		if (fds[i].queue != MSG_QUEUE_NULL) {
			mq = get_backend(fds[i].queue);
			fds[i].revents = mq->event_flag & fds[i].events;
			if (fds[i].revents > 0) {
				count_poll_ready++;
			}
			// If the queue handle is a reader or writer handle,
			// MQPOLL_NOWRITERS or MQPOLL_NOREADERS is reported
			if ((MSG_QUEUE_READER | MSG_QUEUE_WRITER) & q_flags) {
				int new_event = (MQPOLL_NOWRITERS | MQPOLL_NOREADERS) & q_flags;
				fds[i].revents = fds[i].revents | new_event;
			}
		}
	}
	mutex_unlock(&wait_thread->poll_mutex);
	
	//remove thread from queue to do actions
	for (size_t i = 0; i < nfds; ++i){
		if(fds[i].queue != MSG_QUEUE_NULL){
			mq = get_backend(fds[i].queue);
			list_head *a_list = &mq->wait_q;
			mutex_lock(&mq->mq_mutex);
			list_del(a_list, &wait_threads_entry[i].lentry);
			mutex_unlock(&mq->mq_mutex);
		}
	}
	cond_destroy(&wait_thread->poll_ready);

	//free the memory
	free(wait_thread);
	free(wait_threads_entry);

	// return the number of message queues with any triggered events
	return count_poll_ready;
}