import threading

num_of_reps = 1000

def append_hello(lst):
    lst.append("Hello")

my_list = []
threads = []
for i in range(num_of_reps):
    thread = threading.Thread(target=append_hello, args=(my_list,))
    threads.append(thread)

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

print(my_list)
