arg2=$1
arg3=$2
declare -i nprocs=${arg1:=32}
declare -i ncalls=${arg2:=128}
declare -i counter=0
echo "Starting " $nprocs " processes, each exercising " $server " with " $ncalls " complex queries"
until test $counter -ge $nprocs;
do counter=$counter+1; \
   ../bin/fdexec exercise-brico.scm $ncalls &
done

