from cassandra.cluster import Cluster
import sys


toolbar_width = 10

print ""
# setup toolbar
sys.stdout.write("CR6:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['




cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

districts = session.execute('SELECT d_id  FROM district')
i = 0
result = [True] * 100
for district in districts:
    d_id=district.d_id
    orders = session.execute('select o_id,o_ol_cnt from oorder WHERE o_w_id=1 and o_d_id=' + str(d_id) + ' ALLOW FILTERING')
    for order in orders:
        o_id = order.o_id
        o_ol_cnt = order.o_ol_cnt
        order_lines = session.execute('select count(*) from order_line where ol_w_id=1 and ol_d_id=' + str(d_id) + ' and ol_o_id=' + str(o_id)+ ' ALLOW FILTERING')
        for order_line in order_lines:
            order_line_count=order_line[0]
        result[i] = (result[i]) and (o_ol_cnt==order_line_count)
        if o_ol_cnt!=order_line_count:
            print ""
            print "d_id:",d_id
            print "o_id:", o_id
            print "o_ol_cnt:",o_ol_cnt
            print "order_line_count",order_line_count
            print ""
            break
    i=i+1
    sys.stdout.write("-")
    sys.stdout.flush()
            
sys.stdout.write("\n")
            
if all(result): 
    print ">> preserved"
else:
    print ">> violated"


