use tokio::net::{TcpListener, TcpStream};

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

// -----------------------------------------------------------------------
#[tokio::main]
async fn main() {
	let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
	println!("--- Start Listening -> port: 6379");
	let db: Db = Arc::new(Mutex::new(HashMap::new()));

	loop {
		// タプルの2つ目の要素は、新しいコネクションのIPとポートの情報を含んでいる
		let (socket, _) = listener.accept().await.unwrap();
		let db = db.clone();
		
		println!("--- Accepted");
		tokio::spawn(async move {
			process(socket, db).await;
		});
	}
}

// -----------------------------------------------------------------------
async fn process(socket: TcpStream, db: Db) {
	use mini_redis::Command::{Get, Set};

	// ソケットから来るフレームをパースする
	let mut connection = mini_redis::Connection::new(socket);

	// コネクションからコマンドを受け取るため `read_frame` を使う
	while let Some(frame) = connection.read_frame().await.unwrap() {
		let response = match mini_redis::Command::from_frame(frame).unwrap() {
			Set(cmd) => {
				let mut db = db.lock().unwrap();
				db.insert(cmd.key().to_string(), cmd.value().clone());
				mini_redis::Frame::Simple("OK".to_string())
			}
			Get(cmd) => {
				let db = db.lock().unwrap();
				if let Some(value) = db.get(cmd.key()) {
					// `Frame::Bulk` はデータが Bytes` 型であることを期待する
					// とりあえずここでは `.into()` を使って `&Vec<u8>` から `Bytes` に変換する
					mini_redis::Frame::Bulk(value.clone())
				} else {
					mini_redis::Frame::Null
				}
			}
			cmd => panic!("unimplemented {:?}", cmd),
		};

		// クライアントへレスポンスを書き込む
		connection.write_frame(&response).await.unwrap();
	}
}
