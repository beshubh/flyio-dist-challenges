use glob::glob;
use std::collections::HashMap;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufReader, Write, prelude::*};
use std::path::Path;

use anyhow::Context;
use flyio_dist::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicUsize;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Send {
        #[serde(rename = "key")]
        topic: String,
        #[serde(rename = "msg")]
        message: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        #[serde(rename = "msgs")]
        messages: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

#[derive(Debug)]
struct FileHandle {
    r: File,
    w: File,
}

struct KafkaNode {
    id: String,
    node_ids: Vec<String>,
    msg_id_seq: usize,

    // keys in fly-io kafka workload
    topics: Vec<String>,
    next_offsets: HashMap<String, AtomicUsize>,
    file_handles: HashMap<String, FileHandle>,
    // index for message offset -> file_ptr
    index: HashMap<String, HashMap<usize, u64>>,
}

impl KafkaNode {
    fn get_or_create_log_file(
        &mut self,
        topic: &str,
    ) -> anyhow::Result<(&mut FileHandle, &mut AtomicUsize)> {
        if !self.file_handles.contains_key(topic) {
            let str_path = format!("{}-{}.log", self.id, topic);
            let path = Path::new(&str_path);

            if let Some(parent) = path.parent() {
                create_dir_all(parent).context("create all dir, file handles")?; // idempotent: OK if it already exists
            }
            // Open for read/write; create if missing
            let w = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .context("open, append only write log file")?;
            let r = OpenOptions::new()
                .read(true)
                .open(&path)
                .context("open, read only log")?;

            // Seek once to the end to initialize the next offset
            if !self.next_offsets.contains_key(topic) {
                self.next_offsets
                    .insert(topic.to_string(), AtomicUsize::new(0));
            }

            self.file_handles
                .insert(topic.to_string(), FileHandle { r, w });
        }
        Ok((
            self.file_handles.get_mut(topic).unwrap(),
            self.next_offsets.get_mut(topic).unwrap(),
        ))
    }

    fn build_index(
        node_id: &str,
    ) -> anyhow::Result<(
        HashMap<String, HashMap<usize, u64>>,
        HashMap<String, AtomicUsize>,
    )> {
        let pattern = format!("{}-*.log", node_id);

        let mut index: HashMap<String, HashMap<usize, u64>> = HashMap::new();
        let mut next_offsets = HashMap::new();

        for path_entry in glob(&pattern).expect("invalid glob pattern") {
            match path_entry {
                Ok(path) => {
                    if path.is_file() {
                        let readf = File::open(&path).context("build index, read file")?;
                        let mut reader = BufReader::new(readf);

                        // too much confidence in directory structures
                        let stem = path.file_stem().unwrap().to_str().unwrap();
                        let topic = stem.strip_prefix(&format!("{}-", node_id)).unwrap();
                        let mut location_ptr = 0u64;
                        let mut buf = String::new();
                        let mut next_offset = 0;
                        loop {
                            buf.clear();
                            let n = reader.read_line(&mut buf)?;
                            if n == 0 {
                                break;
                            }
                            let log_entry: LogEntry = serde_json::from_str(buf.trim_end())?;
                            if log_entry.offset > next_offset {
                                next_offset = log_entry.offset;
                            }

                            index
                                .entry(topic.to_string())
                                .or_default()
                                .insert(log_entry.offset, location_ptr);
                            location_ptr += n as u64;
                        }
                        next_offsets.insert(topic.to_string(), AtomicUsize::new(next_offset + 1));
                    }
                }
                Err(e) => eprintln!("glob error: {}", e),
            }
        }
        Ok((index, next_offsets))
    }

    fn update_index(&mut self, topic: &str, current_offset: usize, file_loc_ptr: u64) {
        if let Some(entry) = self.index.get_mut(topic) {
            entry.insert(current_offset, file_loc_ptr);
        } else {
            self.index.insert(topic.to_string(), HashMap::new());
            let Some(entry) = self.index.get_mut(topic) else {
                panic!("unreachable");
            };
            entry.insert(current_offset, file_loc_ptr);
        }
    }

    fn append_message(&mut self, topic: &str, message: usize) -> anyhow::Result<usize> {
        let (fh, offset) = self
            .get_or_create_log_file(topic)
            .context("open/seek file")?;
        let mut file = &fh.w;
        let current_offset = *offset.get_mut();
        let entry = LogEntry {
            offset: current_offset,
            message,
        };
        let mut buf = Vec::new();
        let start_ptr = fh.r.metadata()?.len();
        serde_json::to_writer(&mut buf, &entry).context("serialize entry")?;
        buf.push(b'\n');

        *offset.get_mut() += 1; // increment the atomic counter of msg offsets
        // this will append we can we have opened the file in append mode.

        file.write_all(&buf).context("write log entry to file")?;

        // update the index with start ptr of current message.
        self.update_index(topic, current_offset, start_ptr);
        Ok(current_offset)
    }

    fn read_messages(
        &mut self,
        topic: &str,
        start_message_offset: usize,
    ) -> anyhow::Result<Vec<(usize, usize)>> {
        let Some(entry) = self.index.get(topic) else {
            // we don't even have this topic, so offset is definitely not there
            return Ok(vec![]);
        };

        let Some(pos) = entry.get(&start_message_offset) else {
            // we have not processed this entry yet.
            return Ok(vec![]);
        };
        let pos = *pos;
        let (fh, _) = self.get_or_create_log_file(topic)?;
        let rf = &fh.r;
        let mut reader = BufReader::new(rf);

        reader.seek(std::io::SeekFrom::Start(pos))?;

        let mut out = Vec::new();
        let mut started = false;
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: LogEntry = serde_json::from_str(&line)?;
            if !started {
                if entry.offset == start_message_offset {
                    out.push(entry);
                }
                started = true;
            } else {
                out.push(entry);
            }
        }
        let out: Vec<(usize, usize)> = out
            .iter()
            .map(|e| (e.offset, e.message))
            .collect::<Vec<_>>();

        Ok(out)
    }

    fn commit(&mut self, topic: &str, commit_offset: usize) -> anyhow::Result<()> {
        let str_path = format!("{}-{}", self.id, topic);

        let path = Path::new(&str_path);
        if let Some(parent) = path.parent() {
            create_dir_all(parent).context("unable to create all dir")?; // idempotent: OK if it already exists
        }

        std::fs::write(path, format!("{commit_offset}\n")).context("write commit to file")?;
        Ok(())
    }

    fn read_commit(&mut self, topic: &str) -> Option<usize> {
        let path = format!("{}-{}", self.id, topic);
        let s = match std::fs::read_to_string(path).context("read from commit file") {
            Ok(s) => Some(s),
            Err(_) => None,
        };
        if s.is_some() {
            return Some(
                s.unwrap()
                    .trim()
                    .parse()
                    .expect("invalid integer in commit file"),
            );
        }
        None
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct LogEntry {
    offset: usize,
    message: usize,
}

impl Node<(), Payload> for KafkaNode {
    fn from_init(_init_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let mut new = Self {
            id: init.node_id,
            msg_id_seq: 1,
            topics: vec![],
            next_offsets: HashMap::new(),
            file_handles: HashMap::new(),
            index: HashMap::new(),
            node_ids: init.node_ids,
        };
        (new.index, new.next_offsets) = Self::build_index(&new.id).context("building index")?;

        Ok(new)
    }

    fn step(
        &mut self,
        input: Message<Payload>,
        writer: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = input.to_reply(Some(&mut self.msg_id_seq));
        match reply.body.payload {
            Payload::Send { topic, message } => {
                let ofs = self.append_message(&topic, message)?;
                reply.body.payload = Payload::SendOk { offset: ofs };
                reply.send(writer).context("write to stdout, sendok")?;
            }
            Payload::Poll { offsets } => {
                let mut result = HashMap::new();
                for (topic, start_offset) in &offsets {
                    let v = self.read_messages(topic, *start_offset)?;
                    result.insert(topic.to_string(), v);
                }
                reply.body.payload = Payload::PollOk { messages: result };
                reply.send(writer).context("write to stdout, pollok")?;
            }
            Payload::CommitOffsets { offsets } => {
                for (topic, commit_offset) in offsets {
                    self.commit(&topic, commit_offset)?;
                }
                reply.body.payload = Payload::CommitOffsetsOk;
                reply
                    .send(writer)
                    .context("write to stdout, commitoffsetok")?;
            }
            Payload::ListCommittedOffsets { keys } => {
                let mut commits = HashMap::new();

                for top in &keys {
                    let Some(v) = self.read_commit(&top) else {
                        continue;
                    };

                    commits.entry(top.to_string()).or_insert(v);
                }
                reply.body.payload = Payload::ListCommittedOffsetsOk { offsets: commits };
                reply
                    .send(writer)
                    .context("write to stdout, listcommitsok")?;
            }
            _ => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<(), KafkaNode, Payload>(())?;
    Ok(())
}
