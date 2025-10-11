use flyio_dist::*;
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize,
}

impl Node<(), Payload> for EchoNode {
    fn from_init(_state: (), _init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { id: 1 })
    }

    fn step(&mut self, input: Message<Payload>, writer: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.to_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Echo { echo } => reply.body.payload = Payload::EchoOk { echo },

            _ => {}
        }
        reply.send(writer)?;
        Ok(())
    }
}

pub fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, _>(())?;
    Ok(())
}
