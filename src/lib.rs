use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::{
    io::{BufRead, StdoutLock, Write},
    sync::mpsc,
    thread,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

impl<Payload: Debug> Message<Payload> {
    pub fn to_reply(self, msg_id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                msg_id: msg_id.map(|m| {
                    let mid = *m;
                    *m += 1;
                    mid
                }),
                in_reply_to: self.body.msg_id,
                payload: self.body.payload,
            },
        }
    }
    pub fn send(&self, writer: &mut impl Write) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *writer, &self).context("serialize response to message")?;
        writer.write_all(b"\n").context("write new line")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Body<Payload> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Node<S, Payload> {
    fn from_init(init_state: S, init: Init) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn step(&mut self, message: Message<Payload>, writer: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn main_loop<S, N, P>(init_state: S) -> anyhow::Result<()>
where
    N: Node<S, P> + Send,
    P: DeserializeOwned + Send + 'static + Debug,
{
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("no init message received")
            .context("failed to read init message from stdin")?,
    )
    .context("init message cound not be deserialized")?;

    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("first message should be an init message");
    };
    let mut node: N = Node::from_init(init_state, init).context("node initialization failed")?;
    let init_reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            msg_id: Some(0),
            in_reply_to: init_msg.body.msg_id,
            payload: InitPayload::InitOk,
        },
    };
    serde_json::to_writer(&mut stdout, &init_reply).context("error serializing respose to init")?;
    stdout
        .write_all(b"\n")
        .context("error writing new line to stdout")?;
    drop(stdin);
    let (tx, rx) = mpsc::channel();
    let tx_std = tx.clone();
    let jh = thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let line = line.expect("error reading next line from stdin");
            // println!("input received: {:?}", line);
            let input: Message<P> = serde_json::from_str(&line)
                .context("input could not be deserialized")
                .unwrap();

            // println!("input received: {:?}", &input);
            if let Err(e) = tx_std.send(input) {
                eprintln!("error sending input to tx: {e:?}");
            }
        }
    });

    for msg in rx {
        node.step(msg, &mut stdout).unwrap();
    }
    drop(tx);
    jh.join().unwrap();
    Ok(())
}
