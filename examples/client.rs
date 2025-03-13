use mini_redis::{client, Result};
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;

// -----------------------------------------------------------------------
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[derive(Debug)]
enum Command {
	Get {
		key: String,
		resp: Responder<Option<Bytes>>,
	},
	Set {
		key: String,
		val: Bytes,
		resp: Responder<()>,
	}
}

// -----------------------------------------------------------------------
#[tokio::main]
pub async fn main() -> Result<()> {
	set_test_key_value().await;
	
	// チャネルの生成
	let (tx, mut rx) = mpsc::channel(32);

	// ---------------------------------------------
	let manager = tokio::spawn(async move {
		let mut client = client::connect("127.0.0.1:6379").await.unwrap();

		// メッセージの受信を開始
		while let Some(cmd) = rx.recv().await {
			match cmd {
				Command::Get { key, resp } => {
					let res = client.get(&key).await;
					// oneshot で値を送る場合、受信側が既に drop されていたら、結果は Err となる
					// fn send(self, t: T) -> Result<(), T>
					let _ = resp.send(res);  // 手抜き：エラーは無視
				}
				Command::Set { key, val, resp } => {
					let res = client.set(&key, val).await;
					let _ = resp.send(res);  // 手抜き：エラーは無視
				}
			}
		}
	});

	// ---------------------------------------------
	let tx2 = tx.clone();

	let t1 = tokio::spawn(async move {
		let (resp_tx, resp_rx) = oneshot::channel();
		let cmd = Command::Get {
			key: "hello".to_string(),
			resp: resp_tx,
		};
		tx.send(cmd).await.unwrap();
		
		// レスポンスの表示
		let res = resp_rx.await;
		println!("GOT = {:?}", res);
	});

	let t2 = tokio::spawn(async move {
		let (resp_tx, resp_rx) = oneshot::channel();
		let cmd = Command::Set {
			key: "foo".to_string(),
			val: "bar".into(),
			resp: resp_tx,
		};
		tx2.send(cmd).await.unwrap();

		// レスポンスの表示
		let res = resp_rx.await;
		println!("GOT = {:?}", res);
	});

	// ---------------------------------------------
	t1.await.unwrap();
	t2.await.unwrap();
	manager.await.unwrap();

	Ok(())
}

// -----------------------------------------------------------------------
async fn set_test_key_value()
{
	let mut client = client::connect("127.0.0.1:6379").await.unwrap();
	client.set("hello", "world".into()).await.unwrap();
}
