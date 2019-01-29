from cassandra.cluster import Cluster
import sys


toolbar_width = 1

print ""
# setup toolbar
sys.stdout.write("CR8:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['


cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

warehouses = session.execute('SELECT w_id,w_ytd  FROM warehouse')
i = 0
result = [True] * 100
for warehouse in warehouses:
    w_id=warehouse.w_id
    w_ytd=warehouse.w_ytd
    #print "warehouse #" + str(w_id)
    #print "w_ytd         =",w_ytd
    histories = session.execute('select sum(h_amount) from history WHERE H_w_id='+str(w_id) +' ALLOW FILTERING');
    for history in histories:
        sum_h_amount = history[0]
    #print "sum(h_amount) =",sum_h_amount
    #print result[i]
    result[i] = (sum_h_amount==w_ytd)
    i=i+1
    sys.stdout.write("-")
    sys.stdout.flush()

sys.stdout.write("\n")
            
if all(result): 
    print ">> preserved"
else:
    print ">> violated"



