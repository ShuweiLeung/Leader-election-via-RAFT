# Raft-leader-election

### Config file

The configuration file indicates (1) how many instances of your server will run, (2) where those instances are running. The config file looks like:

```
N: 4
node0: <host>:<port>
node1: <host>:<port>
node2: <host>:<port>
node3: <host>:<port>
```

N specifies the number of nodes, which can be up to 9. Then there is a line for each node, starting at 0 and going up to (N-1).

### Running your program

The program should take two arguments: the first is the location of the configuration file, and the second is an indication of which node the program represents. For example:

```
$ python3 raftnode.py ./myconfig.txt 3
```

would start node 3

### Timing

When a majority of the nodes are up and running and can communicate with each other, one of those nodes can be elected as leader within 5 seconds. 
