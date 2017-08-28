FROM debian

env RUST_LOG=hlcup1=info
env THREADS=4
env DATA_PATH=/tmp/data/data.zip
env LISTEN=0.0.0.0:80

ADD target/release/hlcup1 /usr/bin/

CMD hlcup1