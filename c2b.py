import sys
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-q", "--quiet",
                    action="store_false", dest="verbose", default=True,
                    help="don't print status messages to stdout")
args = parser.parse_args()

def my_print(msg):
    if args.verbose:
        print msg



toolbar_width = 10

print ""
# setup toolbar
sys.stdout.write("CR2b:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['


from cassandra.cluster import Cluster
cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

districts = session.execute('SELECT d_id,d_next_o_id FROM district where d_w_id=1')
i = 0
result = [True] * 100
for district in districts:
    d_id=district.d_id
    my_print ("")
    my_print ("district #" + str(d_id))
    d_next_o_id=district.d_next_o_id
    expected_o_id=d_next_o_id-1
    my_print ("d_next_o_id=" + str(d_next_o_id))
    orders = session.execute('select max(no_o_id) from new_order WHERE no_d_id=' + str(d_id) + ' ALLOW FILTERING')
    for order in orders:
        max_o_id = order[0]
    my_print ("max_no_id   =" + str(max_o_id))
    if max_o_id==None:
        continue
    result[i] = (max_o_id==expected_o_id)
    my_print (str(result[i]))
    i = i+1
    sys.stdout.write("-")
    sys.stdout.flush()
            
sys.stdout.write("\n")
            
if all(result): 
    print ">> preserved"
else:
    print ">> violated"

