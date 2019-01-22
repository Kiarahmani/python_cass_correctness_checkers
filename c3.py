from cassandra.cluster import Cluster

import sys


toolbar_width = 10

print ""
# setup toolbar
sys.stdout.write("CR3:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['




cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

districts = session.execute('SELECT d_id  FROM district')
i = 0
result = [True] * 100
for district in districts:
    d_id=district.d_id
    #print "district #" + str(d_id)
    
    orders = session.execute('select max(no_o_id) from new_order WHERE no_d_id=' + str(d_id) + ' ALLOW FILTERING')
    for order in orders:
        max_no_id = order[0]
    #print "max_no_id   =" + str(max_no_id)
    
    orders = session.execute('select min(no_o_id) from new_order WHERE no_d_id=' + str(d_id) + ' ALLOW FILTERING')
    for order in orders:
        min_no_id = order[0]
    #print "min_no_id   =" + str(min_no_id)
    expected_count = max_no_id - min_no_id + 1

    orders = session.execute('select count(no_o_id) from new_order WHERE no_d_id=' + str(d_id) + ' ALLOW FILTERING')
    for order in orders:
        count_no_id = order[0]
    #print "count_no_id =" + str(count_no_id)
    result[i] = (expected_count==count_no_id)
    i = i+1
    sys.stdout.write("-")
    sys.stdout.flush()
            
sys.stdout.write("\n")
            
if all(result): 
    print ">> preserved"
else:
    print ">> violated"


