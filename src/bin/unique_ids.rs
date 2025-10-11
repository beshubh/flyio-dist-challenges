use ::serde::{Deserialize, Serialize};
use anyhow::Context;
use flyio_dist::*;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: String },
}

struct UniqueIdNode {
    msg_id_seq: usize,
    id: String,
}

impl Node<(), Payload> for UniqueIdNode {
    fn from_init(_init_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: init.node_id,
            msg_id_seq: 1,
        })
    }

    fn step(
        &mut self,
        message: Message<Payload>,
        writer: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = message.to_reply(Some(&mut self.msg_id_seq));
        match reply.body.payload {
            Payload::Generate => {
                let unique_id = format!("{}-{}", self.id, self.msg_id_seq);
                reply.body.payload = Payload::GenerateOk { id: unique_id };
                reply.send(writer).context("failed to write to stdout")?;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<(), UniqueIdNode, Payload>(())?;
    Ok(())
}
