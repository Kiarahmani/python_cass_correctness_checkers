from cassandra.cluster import Cluster
import sys
toolbar_width = 1


print ""
# setup toolbar
sys.stdout.write("CR11:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['




cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

result = [True] * 100
i = 0

# since this intially holds only for district 10 we check it only for that district
districts = session.execute('SELECT d_id  FROM district where d_w_id=1')
for district in districts:
    d_id=district.d_id
    orders = session.execute('select count(*) from oorder where o_w_id=1 and o_d_id='+str(d_id)) 
    for order in orders:
        order_count = order[0]
    #print "order_count     =", order_count

    new_orders = session.execute('select count(*) from new_order where no_w_id=1 and no_d_id='+str(d_id))
    for new_order in new_orders:
        new_order_count = new_order[0]
    #print "new_order_count =",new_order_count
    result[i] = (order_count-new_order_count)==2100
    #print result[i]
    i = i+1
    sys.stdout.write("-")
    sys.stdout.flush()
sys.stdout.write("\n")

if all(result):
    print ">> preserved"
else:
    print ">> violated"




