#!/usr/bin/env python 
# 
# Output generated is intended to be piped into the the flume shell
# usage: 
#   gen-config.py [nodes [eventsPerNode [#collecotrs [#agents]]]]
#
# generally the output is piped to the flume shell
#   gen-config.py ... | flume shell

def printFlumeScript (master, agents, collectors, collectorSrc, collectorSink, nodes, eventsPerNode):
  print """
## auto generated flume shell script
# master = %(master)s
connect %(master)s
exec unmapAll

# TODO (jon) should wait until umap takes effect, just wait for a heartbeat instead
exec noop 10000

# collectors = %(collectors)s
""" % { 'master' : master , 'collectors' : collectors}

  benchSink = "'{benchreport(\"report\",\"text(\\\"/tmp/benchmark-%dc-%dax-%dnx%de\\\")\") => %s}'" % (len(collectors), len(agents), nodes, eventsPerNode, collectorSink)


  for (i,c) in enumerate(collectors):
    print "# specifiy collector logical nodes"
    print "exec config collector%02d " % i,
    print "'" + collectorSrc + "'" + " " + benchSink


  for (i,c) in enumerate(collectors):
    print "# spawns collector logical nodes on the physical nodes"
    print "exec spawn %s collector%02d" % (c,i)

  print "# wait for all the collectors to be ACTIVE"
  print "waitForNodesActive 30000",
  for (i,c) in enumerate(collectors):
    print "collector%02d" % i,
  print
 
  print "# specify agent logical nodes"
  for i in range(nodes):
    print "submit config node%02d 'asciisynth(%d)'" % (i,eventsPerNode),
    print "'{benchinject=>rpcSink(\"" + collectors[i%len(collectors)] + "\",35853) } '"

  print "# wait forever until all submitted commands accepted"
  print "wait 0"

  print "# spawn/map logical nodes on to physical nodes"
  nodesPerAgent = (nodes+1)/len(agents)
  for i in range(nodes):
    print "exec spawn "+ agents[i/nodesPerAgent]+" node%02d" % i


  print
  print "# wait for nodes to be active an then inactive"
  print "waitForNodesActive 30000",

  for n in range(nodes):
    print "node%02d" % n,
  print

  print "waitForNodesDone 0",

  for n in range(nodes):
    print "node%02d" % n,
  print

  print "# exit"
  print "quit"


############################################################
# python main voodoo
if __name__ == "__main__":

  import sys

  #import pdb; pdb.set_trace()
  # overrideable values
  nodes = 10
  eventsPerNode=10000000

  ## Some hard coded values to later be made options.
  # physical name for collector machines.
  master = "master"

  agents = [ "agent1", "agent2" ]
  collectors = [ "collector1", "collector2" ]
  collectorSrc = "rpcSource(35853)"
  collectorSink = "null"

  # nodes
  if len(sys.argv)>=2 :
    nodes = int(sys.argv[1])

  # events per node
  if len(sys.argv)>=3 :
    eventsPerNode = int(sys.argv[2])

  # collectors used
  if len(sys.argv)>=4 :
    cols =  int(sys.argv[3])
    if cols < len(collectors):
      collectors = collectors[:cols]

  # agents used
  if len(sys.argv)>=5 :
    ags = int (sys.argv[4])
    if ags < len(agents) :
      agents = agents[:ags]

  printFlumeScript(master, agents, collectors, collectorSrc, collectorSink, nodes, eventsPerNode)
