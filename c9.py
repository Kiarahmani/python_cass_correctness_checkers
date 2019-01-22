from cassandra.cluster import Cluster
import sys


toolbar_width = 10

print ""
# setup toolbar
sys.stdout.write("CR9:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['


cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

districts = session.execute('SELECT d_id,d_ytd  FROM district')
i = 0
result = [True] * 100
for district in districts:
    d_id=district.d_id
    d_ytd=district.d_ytd
    #print "district #" + str(d_id)
    #print "d_ytd         =",d_ytd
    histories = session.execute('select sum(h_amount) from history WHERE H_d_id='+str(d_id) +' ALLOW FILTERING');
    for history in histories:
        sum_h_amount = history[0]
    #print "sum(h_amount) =",sum_h_amount
    i=i+1
    sys.stdout.write("-")
    sys.stdout.flush()

sys.stdout.write("\n")

if all(result):
    print ">> preserved"
else:
    print ">> violated"




