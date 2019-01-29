import sys
from cassandra.cluster import Cluster
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
sys.stdout.write("CR2a:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['

cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

result = [True] * 100
i = 0

districts = session.execute('SELECT d_id,d_next_o_id FROM district')
for district in districts:
    d_id=district.d_id
    my_print ("")
    my_print("district:"+ str(d_id))
    d_next_o_id=district.d_next_o_id
    expected_o_id=d_next_o_id-1
    my_print("d_next_o_id=" + str(d_next_o_id))
    orders = session.execute('select max(o_id) from oorder WHERE o_d_id=' + str(d_id) + ' ALLOW FILTERING')
    for order in orders:
        max_o_id = order[0]
    my_print ("max_o_id   =" + str(max_o_id))
    result[i] = (max_o_id==expected_o_id)
    i = i+1
    sys.stdout.write("-")
    sys.stdout.flush()
            
sys.stdout.write("\n")
            
if all(result): 
    print ">> preserved"
else:
    print ">> violated"

