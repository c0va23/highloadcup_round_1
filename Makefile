builds:
	mkdir builds

app: src/ builds
	cargo build --release
	cp target/release/hlcup1 builds/

image: app
	docker build -t stor.highloadcup.ru/travels/rabbit_worker .

publish: image
	docker push stor.highloadcup.ru/travels/rabbit_worker
