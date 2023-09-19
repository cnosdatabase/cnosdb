use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alpha1, alphanumeric1, none_of, space0};
use nom::combinator::{map_parser, map_res, recognize};
use nom::multi::{many0_count, many1};
use nom::sequence::{delimited, pair, tuple};
use nom::IResult;

#[derive(Debug, Clone)]
pub struct Instruction {
    /// set the requested tenant
    tenant_name: String,
    /// set the requested database
    db_name: String,
    /// set sort or not
    sort: bool,
    /// set pretty or not
    pretty: bool,
    /// set user_name
    user_name: String,
    /// set how long to timeout
    time_out: Option<u64>,

    sleep: Option<u64>,

    precision: Option<String>,

    chunked: Option<bool>,
}

impl Default for Instruction {
    fn default() -> Self {
        Self {
            tenant_name: "cnosdb".to_string(),
            db_name: "public".to_string(),
            sort: false,
            pretty: true,
            user_name: "root".to_string(),
            time_out: None,
            sleep: None,
            precision: None,
            chunked: Some(false),
        }
    }
}
fn instruction_parse_str<'a>(
    instruction_name: &'a str,
) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str> {
    delimited(
        tuple((
            tag("--"),
            space0,
            tag("#"),
            tag_no_case(instruction_name),
            space0,
            tag("="),
            space0,
        )),
        recognize(many1(none_of(" \t\n\r"))),
        space0,
    )
}

fn instruction_parse_identity<'a>(
    instruction_name: &'a str,
) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str> {
    map_parser(
        instruction_parse_str(instruction_name),
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0_count(alt((alphanumeric1, tag("_")))),
        )),
    )
}

fn instruction_parse_to<'a, T: FromStr>(
    instruction_name: &'a str,
) -> impl FnMut(&'a str) -> IResult<&'a str, T> {
    map_res(instruction_parse_str(instruction_name), |s: &str| {
        s.parse::<T>()
    })
}

impl Instruction {
    pub fn tenant_name(&self) -> &str {
        &self.tenant_name
    }

    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    pub fn sort(&self) -> bool {
        self.sort
    }

    pub fn pretty(&self) -> bool {
        self.pretty
    }

    pub fn user_name(&self) -> &str {
        &self.user_name
    }

    pub fn time_out(&self) -> Option<u64> {
        self.time_out
    }

    pub fn sleep(&self) -> Option<u64> {
        self.sleep
    }

    pub fn precision(&self) -> Option<&str> {
        self.precision.as_deref()
    }

    pub fn chunked(&self) -> &str {
        if self.chunked.unwrap_or(true) {
            "true"
        } else {
            "false"
        }
    }
    /// parse line to modify instruction
    pub fn parse_and_change(&mut self, line: &str) {
        if let Ok((_, tenant_name)) = instruction_parse_identity("TENANT")(line) {
            self.tenant_name = tenant_name.to_string();
        }

        if let Ok((_, dbname)) = instruction_parse_identity("DATABASE")(line) {
            self.db_name = dbname.to_string();
        }

        if let Ok((_, user_name)) = instruction_parse_identity("USER_NAME")(line) {
            self.user_name = user_name.to_string();
        }

        if let Ok((_, pretty)) = instruction_parse_to::<bool>("PRETTY")(line) {
            self.pretty = pretty;
        }

        if let Ok((_, sort)) = instruction_parse_to::<bool>("SORT")(line) {
            self.sort = sort;
        }

        if let Ok((_, timeout)) = instruction_parse_to::<u64>("TIMEOUT")(line) {
            self.time_out = Some(timeout)
        }

        if let Ok((_, slepp)) = instruction_parse_to::<u64>("SLEEP")(line) {
            self.sleep = Some(slepp)
        }

        if let Ok((_, precision)) = instruction_parse_identity("PRECISION")(line) {
            self.precision = Some(precision.to_string())
        }

        if let Ok((_, chunked)) = instruction_parse_to::<bool>("CHUNKED")(line) {
            self.chunked = Some(chunked)
        }
    }
}

/// one Query
#[derive(Debug, Clone)]
pub struct Query {
    instruction: Instruction,
    query: String,
}

impl Query {
    pub fn is_stream_respones(&self) -> bool {
        self.instruction.chunked() == "true"
    }
}

#[derive(Clone, Debug)]
pub struct LineProtocol {
    instruction: Instruction,
    lines: String,
}

pub struct LineProtocolBuild {
    lines: String,
}

impl LineProtocolBuild {
    pub fn finish(self, instruction: Instruction) -> LineProtocol {
        LineProtocol {
            instruction,
            lines: self.lines,
        }
    }
    pub fn new() -> LineProtocolBuild {
        LineProtocolBuild {
            lines: String::new(),
        }
    }
    pub fn push(&mut self, line: &str) {
        self.lines.push_str(line);
        self.lines.push('\n');
    }

    pub fn finished(&self) -> bool {
        self.lines.is_empty()
    }
}

impl LineProtocol {
    pub fn as_str(&self) -> &str {
        self.lines.as_str()
    }
    pub fn instruction(&self) -> &Instruction {
        &self.instruction
    }
}

#[derive(Clone, Debug)]
pub struct OpenTSDBProtocol {
    instruction: Instruction,
    lines: String,
}

pub struct OpenTSDBProtocolBuild {
    lines: String,
}

impl OpenTSDBProtocolBuild {
    pub fn finish(self, instruction: Instruction) -> OpenTSDBProtocol {
        OpenTSDBProtocol {
            instruction,
            lines: self.lines,
        }
    }

    pub fn new() -> OpenTSDBProtocolBuild {
        OpenTSDBProtocolBuild {
            lines: String::new(),
        }
    }

    pub fn push(&mut self, line: &str) {
        self.lines.push_str(line);
        self.lines.push('\n');
    }

    pub fn finished(&self) -> bool {
        self.lines.is_empty()
    }
}

impl OpenTSDBProtocol {
    pub fn as_str(&self) -> &str {
        self.lines.as_str()
    }
    pub fn instruction(&self) -> &Instruction {
        &self.instruction
    }
}

#[derive(Clone, Debug)]
pub struct OpenTSDBJson {
    instruction: Instruction,
    lines: String,
}

pub struct OpenTSDBJsonBuild {
    lines: String,
}

impl OpenTSDBJsonBuild {
    pub fn finish(self, instruction: Instruction) -> OpenTSDBJson {
        OpenTSDBJson {
            instruction,
            lines: self.lines,
        }
    }

    pub fn new() -> OpenTSDBJsonBuild {
        OpenTSDBJsonBuild {
            lines: String::new(),
        }
    }

    pub fn push(&mut self, line: &str) {
        self.lines.push_str(line);
        self.lines.push('\n');
    }

    pub fn finished(&self) -> bool {
        self.lines.is_empty()
    }
}

impl OpenTSDBJson {
    pub fn as_str(&self) -> &str {
        self.lines.as_str()
    }
    pub fn instruction(&self) -> &Instruction {
        &self.instruction
    }
}

#[derive(Clone, Debug)]
pub struct ShellScript {
    instruction: Instruction,
    lines: String,
}

impl ShellScript {
    pub fn as_str(&self) -> &str {
        self.lines.as_str()
    }
    pub fn instruction(&self) -> &Instruction {
        &self.instruction
    }
}

pub struct ShellScriptBuild {
    lines: String,
}

impl ShellScriptBuild {
    pub fn finish(self, instruction: Instruction) -> ShellScript {
        ShellScript {
            instruction,
            lines: self.lines,
        }
    }

    pub fn new() -> ShellScriptBuild {
        ShellScriptBuild {
            lines: String::new(),
        }
    }

    pub fn push(&mut self, line: &str) {
        self.lines.push_str(line);
        self.lines.push('\n');
    }

    pub fn finished(&self) -> bool {
        self.lines.is_empty()
    }
}

#[derive(Clone, Debug)]
pub enum DBRequest {
    Query(Query),
    LineProtocol(LineProtocol),
    OpenTSDBProtocol(OpenTSDBProtocol),
    OpenTSDBJson(OpenTSDBJson),
    ShellScript(ShellScript),
}

impl DBRequest {
    pub fn parse_requests(lines: &str) -> Vec<DBRequest> {
        let mut requests = Vec::<DBRequest>::new();
        let mut instruction = Instruction::default();

        let mut query_build = QueryBuild::new();
        let mut parsing_line_protocol = false;
        let mut parsing_opentsdb_protocol = false;
        let mut parsing_opentsdb_json = false;
        let mut parsing_shell_script = false;

        let mut line_protocol_build = LineProtocolBuild::new();
        let mut open_tsdb_protocol_build = OpenTSDBProtocolBuild::new();
        let mut open_tsdb_json_build = OpenTSDBJsonBuild::new();
        let mut shell_script_build = ShellScriptBuild::new();

        for line in lines.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if line.starts_with("--#LP_BEGIN") {
                parsing_line_protocol = true;
                continue;
            }
            if line.starts_with("--#LP_END") {
                parsing_line_protocol = false;
                requests.push(DBRequest::LineProtocol(
                    line_protocol_build.finish(instruction.clone()),
                ));
                line_protocol_build = LineProtocolBuild::new();
                continue;
            }

            if line.starts_with("--#OPENTSDB_BEGIN") {
                parsing_opentsdb_protocol = true;
                continue;
            }

            if line.starts_with("--#OPENTSDB_END") {
                parsing_opentsdb_protocol = false;
                requests.push(DBRequest::OpenTSDBProtocol(
                    open_tsdb_protocol_build.finish(instruction.clone()),
                ));
                open_tsdb_protocol_build = OpenTSDBProtocolBuild::new();
                continue;
            }

            if line.starts_with("--#OPENTSDB_JSON_BEGIN") {
                parsing_opentsdb_json = true;
                continue;
            }

            if line.starts_with("--#OPENTSDB_JSON_END") {
                parsing_opentsdb_json = false;
                requests.push(DBRequest::OpenTSDBJson(
                    open_tsdb_json_build.finish(instruction.clone()),
                ));
                open_tsdb_json_build = OpenTSDBJsonBuild::new();
                continue;
            }

            if line.starts_with("--#SHELL_SCRIPT_BEGIN") {
                parsing_shell_script = true;
                continue;
            }

            if line.starts_with("--#SHELL_SCRIPT_END") {
                parsing_shell_script = false;
                requests.push(DBRequest::ShellScript(
                    shell_script_build.finish(instruction.clone()),
                ));
                shell_script_build = ShellScriptBuild::new();
                continue;
            }

            if line.starts_with("--") {
                instruction.parse_and_change(line);
                continue;
            }

            if parsing_line_protocol {
                line_protocol_build.push(line);
            } else if parsing_opentsdb_protocol {
                open_tsdb_protocol_build.push(line);
            } else if parsing_opentsdb_json {
                open_tsdb_json_build.push(line);
            } else if parsing_shell_script {
                shell_script_build.push(line);
            } else {
                query_build.push_str(line);
                if line.ends_with(';') {
                    let query = query_build.finish(instruction.clone());
                    requests.push(DBRequest::Query(query));
                    query_build = QueryBuild::new();
                }
            }
        }
        if !query_build.finished() {
            let query = query_build.finish(instruction.clone());
            requests.push(DBRequest::Query(query));
        }

        if !line_protocol_build.finished() {
            let line_protocol = line_protocol_build.finish(instruction.clone());
            requests.push(DBRequest::LineProtocol(line_protocol));
        }

        if !open_tsdb_protocol_build.finished() {
            let open_tsdb_protocol = open_tsdb_protocol_build.finish(instruction.clone());
            requests.push(DBRequest::OpenTSDBProtocol(open_tsdb_protocol));
        }

        if !open_tsdb_json_build.finished() {
            let open_tsdb_json = open_tsdb_json_build.finish(instruction.clone());
            requests.push(DBRequest::OpenTSDBJson(open_tsdb_json));
        }

        if !shell_script_build.finished() {
            let shell_script = shell_script_build.finish(instruction.clone());
            requests.push(DBRequest::ShellScript(shell_script));
        }

        requests
    }
}

impl Query {
    pub fn as_str(&self) -> &str {
        &self.query
    }

    pub fn instruction(&self) -> &Instruction {
        &self.instruction
    }

    pub fn is_return_result_set(&self) -> bool {
        let lowercase = self.query.trim().to_lowercase();
        lowercase.starts_with("select")
            || lowercase.starts_with("show")
            || lowercase.starts_with("insert")
    }
}

pub struct QueryBuild {
    buffer: String,
}

impl QueryBuild {
    pub fn new() -> QueryBuild {
        QueryBuild {
            buffer: String::new(),
        }
    }

    pub fn push_str(&mut self, line: &str) -> &mut Self {
        let line = line.trim();
        if line.is_empty() {
            return self;
        }

        if !self.buffer.is_empty() {
            self.buffer.push(' ');
        }

        self.buffer.push_str(line);

        self
    }

    pub fn finish(&mut self, instruction: Instruction) -> Query {
        if !self.buffer.ends_with(';') {
            self.buffer.push(';');
        }

        let res = Query {
            instruction,
            query: self.buffer.clone(),
        };

        self.buffer.clear();
        res
    }

    pub fn finished(&self) -> bool {
        self.buffer.is_empty()
    }
}

#[test]
fn test_query_build() {
    let mut build = QueryBuild::new();
    let query = build
        .push_str("Select * ")
        .push_str("From table\n")
        .finish(Instruction::default());
    println!("{}", query.as_str());
}

#[test]
fn test_parse_instruction() {
    let mut instruction = Instruction::default();

    let line = r##"--#DATABASE = _abc_"##;
    instruction.parse_and_change(line);
    assert_eq!(instruction.db_name, "_abc_");

    let line = r##"--#USER_NAME = hello"##;
    instruction.parse_and_change(line);
    assert_eq!(instruction.user_name, "hello");

    let line = r##"--#SORT = true"##;
    instruction.parse_and_change(line);
    assert!(instruction.sort);

    let line = r##"--#SORT = false"##;
    instruction.parse_and_change(line);
    assert!(!instruction.sort);

    assert_eq!(instruction.time_out, None);
    let line = r##"--#TIMEOUT = 10"##;
    instruction.parse_and_change(line);
    assert_eq!(instruction.time_out, Some(10));
}

#[test]
fn test_queries_parse() {
    let content = r##"
    -- #DATABASE=hello
    -- #SORT = true
    -- #PRETTY = false
    SElect * from table;


    SELECT name, age
    -- #PREETY = false
    From people;

    "##;
    let db_requests = DBRequest::parse_requests(content);
    println!("{:#?}", &db_requests);
}
