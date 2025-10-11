use std::{collections::HashMap, vec};

use anyhow::Context;
use flyio_dist::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct BroadcastNode {
    id: String,
    node_ids: Vec<String>,
    msg_id_seq: usize,
    seen_messages: Vec<usize>,
    topology: HashMap<String, Vec<String>>,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(_init_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node = Self {
            id: init.node_id,
            node_ids: init.node_ids,
            msg_id_seq: 1,
            seen_messages: vec![],
            topology: HashMap::new(),
        };
        Ok(node)
    }

    fn step(
        &mut self,
        input: Message<Payload>,
        writer: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()>
    where
        Payload: Clone,
    {
        let mut reply = input.clone().to_reply(Some(&mut self.msg_id_seq));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                for node in &self.node_ids {
                    if node == &self.id {
                        continue;
                    }
                    let mut m = input.clone();
                    m.dst = node.clone();
                    m.send(writer)
                        .context("failed to broadcast messages to the nodes")?;
                }

                self.seen_messages.push(message);

                reply.body.payload = Payload::BroadcastOk;
                reply
                    .send(writer)
                    .context("failed to write msg to std out, broadcast ok")?;
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.seen_messages.clone(),
                };
                reply
                    .send(writer)
                    .context("failed to write msg to stdout, read ok")?;
            }
            Payload::Topology { topology } => {
                self.topology = topology;
                reply.body.payload = Payload::TopologyOk;
                reply
                    .send(writer)
                    .context("faield to write msg to stdout, topologyok")?;
            }
            Payload::ReadOk { .. } | Payload::BroadcastOk | Payload::TopologyOk => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<(), BroadcastNode, Payload>(())?;
    Ok(())
}
