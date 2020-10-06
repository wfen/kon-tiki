#lein run test --bin raft.py --nodes n1 --rate 1 --time-limit 10
#lein run test --bin raft.py --nodes n1,n2 --rate 1 --time-limit 10
java -jar maelstrom/maelstrom.jar test --bin raft.py --nodes n1,n2,n3 --rate 0 --time-limit 10
