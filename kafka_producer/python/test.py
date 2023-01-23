import threading
import time

def perform_task(task_id):
    msg = f"Starting task {task_id} {threading.get_native_id()} Finished task {task_id}"
    print(msg)

threads = []

# Create 10 threads to perform the task
for i in range(10):
    t = threading.Thread(target=perform_task, args=(i,))
    threads.append(t)

# Start the threads
for t in threads:
    t.start()

# Wait for all threads to complete
for t in threads:
    t.join()

print("All tasks completed")