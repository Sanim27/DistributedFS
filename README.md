To run the code:

Run in three different terminals.
One terminal for master, 2 for clients.

terminal 1 --> go run ./cmd/master                         
terminal 2 --> go run ./cmd/peer -port=:3002 -master=:9000 
terminal 3 --> go run ./cmd/peer -port=:3003 -master=:9000 -upload om.txt 

### not fixed : Download is not working as expected.
terminal 3 --> go run ./cmd/peer -port=:3003 -master=:9000 -download om.txt