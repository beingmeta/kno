import time

def fib_iter(a,b,count):
    if count==0:
        return b
    else:
        return fib_iter(a+b,a,count-1)

def fibtr(n):
    return fib_iter(1,1,n)
        
def time_fibtr(count,n):
    start=time.clock()
    for i in range(count):
        fibtr(n)
    end=time.clock()
    return end-start    

print( fibtr(10) )
print( time_fibtr(5000,10) )

