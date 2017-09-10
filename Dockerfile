FROM debian

env RUST_LOG=hlcup1=error
env DATA_PATH=/tmp/data/data.zip
env LISTEN=0.0.0.0:80
env BACKLOG=10024

ADD builds/hlcup1 /usr/bin/hlcup1

CMD hlcup1