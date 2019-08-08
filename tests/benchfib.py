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

print( "fibtr(10) x 5000 in %s"%time_fibtr(5000,10) )
print( "fibtr(100) x 5000 in %s"%time_fibtr(1000,100) )
print( "fibtr(200) x 5000 in %s"%time_fibtr(500,200) )
#print( "fibtr(500) x 100 in %s"%time_fibtr(100,500) )
