from cassandra.cluster import Cluster
import sys

toolbar_width = 10

print ""
# setup toolbar
sys.stdout.write("CR7:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['

cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

districts = session.execute('SELECT d_id  FROM district')
i = 0
result = [True] * 100
for district in districts:
    d_id=district.d_id
    order_lines = session.execute('select ol_o_id,ol_delivery_d from order_line WHERE ol_w_id=1 and ol_d_id=' + str(d_id) + ' ALLOW FILTERING')
    for order_line in order_lines:
        o_id = order_line.ol_o_id
        ol_delivery_d = order_line.ol_delivery_d
        if ol_delivery_d==None:
            orders = session.execute ('select o_carrier_id from oorder WHERE o_w_id=1 and o_d_id=' + str(d_id) + ' and o_id='+str(o_id))
            for order in orders:
                o_carrier_id=order.o_carrier_id
            if o_carrier_id!=None:
                result[i]=False;
                break
    #print result[i]
    i=i+1
    sys.stdout.write("-")
    sys.stdout.flush()

sys.stdout.write("\n")
            
if all(result): 
    print ">> preserved"
else:
    print ">> violated"


