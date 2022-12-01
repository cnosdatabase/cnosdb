use std::collections::VecDeque;
use std::str::FromStr;

use datafusion::sql::parser::CreateExternalTable;

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::{
    ast::{DataType, Ident, ObjectName, Value},
    dialect::{keywords::Keyword, Dialect, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use models::codec::Encoding;
use snafu::ResultExt;
use spi::query::ast::{
    AlterDatabase, AlterTable, AlterTableAction, ColumnOption, CreateDatabase, CreateTable,
    DatabaseOptions, DescribeDatabase, DescribeTable, DropObject, Explain, ExtStatement,
    ObjectType, ShowSeries,
};
use spi::query::parser::Parser as CnosdbParser;
use spi::query::ParserSnafu;
use trace::debug;

// support tag token
#[derive(Debug, PartialEq, Eq)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum CnosKeyWord {
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    TAGS,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    CODEC,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    TAG,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    FIELD,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    DATABASES,

    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    TTL,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    SHARD,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    VNODE_DURATION,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    REPLICA,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    PRECISION,

    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    QUERIES,

    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    SERIES,
}

impl FromStr for CnosKeyWord {
    type Err = ParserError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "TAGS" => Ok(CnosKeyWord::TAGS),
            "CODEC" => Ok(CnosKeyWord::CODEC),
            "TAG" => Ok(CnosKeyWord::TAG),
            "FIELD" => Ok(CnosKeyWord::FIELD),
            "TTL" => Ok(CnosKeyWord::TTL),
            "SHARD" => Ok(CnosKeyWord::SHARD),
            "VNODE_DURATION" => Ok(CnosKeyWord::VNODE_DURATION),
            "REPLICA" => Ok(CnosKeyWord::REPLICA),
            "PRECISION" => Ok(CnosKeyWord::PRECISION),
            "DATABASES" => Ok(CnosKeyWord::DATABASES),
            "SERIES" => Ok(CnosKeyWord::SERIES),
            _ => Err(ParserError::ParserError(format!(
                "fail parse {} to CnosKeyWord",
                s
            ))),
        }
    }
}

pub type Result<T, E = ParserError> = std::result::Result<T, E>;

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

#[derive(Default)]
pub struct DefaultParser {}

impl CnosdbParser for DefaultParser {
    fn parse(&self, sql: &str) -> spi::query::Result<VecDeque<ExtStatement>> {
        ExtParser::parse_sql(sql).context(ParserSnafu)
    }
}

/// SQL Parser
pub struct ExtParser<'a> {
    parser: Parser<'a>,
}

impl<'a> ExtParser<'a> {
    /// Parse the specified tokens
    pub fn new(sql: &str) -> Result<Self> {
        let dialect = &GenericDialect {};
        ExtParser::new_with_dialect(sql, dialect)
    }
    /// Parse the specified tokens with dialect
    fn new_with_dialect(sql: &str, dialect: &'a dyn Dialect) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(ExtParser {
            parser: Parser::new(tokens, dialect),
        })
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql(sql: &str) -> Result<VecDeque<ExtStatement>> {
        let dialect = &GenericDialect {};
        ExtParser::parse_sql_with_dialect(sql, dialect)
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<VecDeque<ExtStatement>> {
        let mut parser = ExtParser::new_with_dialect(sql, dialect)?;
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }

        debug!("Parser sql:{}, stmts:{:#?}", sql, stmts);

        Ok(stmts)
    }

    /// Parse a new expression
    fn parse_statement(&mut self) -> Result<ExtStatement> {
        match self.parser.peek_token() {
            Token::Word(w) => match w.keyword {
                Keyword::DROP => {
                    self.parser.next_token();
                    self.parse_drop()
                }
                Keyword::DESCRIBE | Keyword::DESC => {
                    self.parser.next_token();
                    self.parse_describe()
                }
                Keyword::SHOW => {
                    self.parser.next_token();
                    self.parse_show()
                }
                Keyword::ALTER => {
                    self.parser.next_token();
                    self.parse_alter()
                }
                Keyword::CREATE => {
                    self.parser.next_token();
                    self.parse_create()
                }
                Keyword::EXPLAIN => {
                    self.parser.next_token();
                    self.parse_explain()
                    // } else {
                    //     Ok(ExtStatement::SqlStatement(Box::new(
                    //         self.parser.parse_statement()?,
                    //     )))
                    // }
                }
                _ => Ok(ExtStatement::SqlStatement(Box::new(
                    self.parser.parse_statement()?,
                ))),
            },
            _ => Ok(ExtStatement::SqlStatement(Box::new(
                self.parser.parse_statement()?,
            ))),
        }
    }
    // Report unexpected token
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    // fn expected_cnos_keyword<T>(&self, expected: &str, found: CnosKeyWord) -> Result<T> {
    //     parser_err!(format!("Expected {}, found: {:?}", expected, found))
    // }

    /// Parse a SQL SHOW statement
    fn parse_show(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::TABLES) {
            self.parse_show_tables()
        } else if self.parse_cnos_keyword(CnosKeyWord::DATABASES) {
            self.parse_show_databases()
        } else if self.parse_cnos_keyword(CnosKeyWord::SERIES) {
            self.parse_show_series()
        } else if self.parse_cnos_keyword(CnosKeyWord::QUERIES) {
            self.parse_show_queries()
        } else {
            self.expected("tables/databases", self.parser.peek_token())
        }
    }

    fn parse_show_series(&mut self) -> Result<ExtStatement> {
        let database_name = if self.parser.parse_keyword(Keyword::ON) {
            Some(self.parser.parse_object_name()?)
        } else {
            None
        };
        self.parser.expect_keyword(Keyword::FROM)?;
        let table = self.parser.parse_object_name()?;
        let selection = if self.parser.parse_keyword(Keyword::WHERE) {
            Some(self.parser.parse_expr()?)
        } else {
            None
        };

        let mut limit = None;
        let mut offset = None;

        for _x in 0..2 {
            if limit.is_none() && self.parser.parse_keyword(Keyword::LIMIT) {
                limit = self.parser.parse_limit()?
            }

            if offset.is_none() && self.parser.parse_keyword(Keyword::OFFSET) {
                offset = Some(self.parser.parse_offset()?)
            }
        }
        Ok(ExtStatement::ShowSeries(Box::new(ShowSeries {
            database_name,
            table,
            selection,
            limit,
            offset,
        })))
    }

    // fn rewrite_show_series(&mut self) -> Result<ExtStatement> {
    //
    //     Statement::Query()
    //     ExtStatement::SqlStatement
    // }
    fn parse_explain(&mut self) -> Result<ExtStatement> {
        let analyze = self.parser.parse_keyword(Keyword::ANALYZE);
        let verbose = self.parser.parse_keyword(Keyword::VERBOSE);
        let mut format = None;
        if self.parser.parse_keyword(Keyword::FORMAT) {
            format = Some(self.parser.parse_analyze_format()?);
        }
        if self.parser.parse_keyword(Keyword::EXPLAIN) {
            return Err(ParserError::ParserError(
                "EXPLAIN can only appear once".to_string(),
            ));
        }
        match self.parse_statement()? {
            ExtStatement::SqlStatement(statement) => {
                Ok(ExtStatement::SqlStatement(Box::new(Statement::Explain {
                    describe_alias: false,
                    analyze,
                    verbose,
                    format,
                    statement,
                })))
            }
            ExtStatement::ShowSeries(statement) => Ok(ExtStatement::Explain(Explain {
                analyze,
                verbose,
                format,
                ext_statement: Box::new(ExtStatement::ShowSeries(statement)),
            })),
            _ => Err(ParserError::ParserError("Not Implemented".to_string())),
        }
    }

    fn parse_show_queries(&mut self) -> Result<ExtStatement> {
        Ok(ExtStatement::ShowQueries)
    }

    fn parse_show_databases(&mut self) -> Result<ExtStatement> {
        Ok(ExtStatement::ShowDatabases())
    }

    fn parse_show_tables(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::ON) {
            let database_name = self.parser.parse_object_name()?;
            Ok(ExtStatement::ShowTables(Some(database_name)))
        } else {
            Ok(ExtStatement::ShowTables(None))
        }
    }

    /// Parse a SQL DESCRIBE DATABASE statement
    fn parse_describe_database(&mut self) -> Result<ExtStatement> {
        debug!("Parse Describe DATABASE statement");
        let database_name = self.parser.parse_object_name()?;

        let describe = DescribeDatabase { database_name };

        Ok(ExtStatement::DescribeDatabase(describe))
    }

    /// Parse a SQL DESCRIBE TABLE statement
    fn parse_describe_table(&mut self) -> Result<ExtStatement> {
        let table_name = self.parser.parse_object_name()?;

        let describe = DescribeTable { table_name };

        Ok(ExtStatement::DescribeTable(describe))
    }

    fn parse_describe(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::TABLE) {
            self.parse_describe_table()
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            self.parse_describe_database()
        } else {
            self.expected("tables/databases", self.parser.peek_token())
        }
    }

    /// Parse a SQL ALTER statement
    fn parse_alter(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::TABLE) {
            self.parse_alter_table()
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            self.parse_alter_database()
        } else {
            self.expected("TABLE or DATABASE", self.parser.peek_token())
        }
    }

    fn parse_alter_table(&mut self) -> Result<ExtStatement> {
        let table_name = self.parser.parse_object_name()?;

        if self.parser.parse_keyword(Keyword::ADD) {
            self.parse_alter_table_add_column(table_name)
        } else if self.parser.parse_keyword(Keyword::ALTER) {
            self.parse_alter_table_alter_column(table_name)
        } else if self.parser.parse_keyword(Keyword::DROP) {
            self.parse_alter_table_drop_column(table_name)
        } else {
            self.expected("ADD or ALTER or DROP", self.parser.peek_token())
        }
    }

    fn parse_alter_table_add_column(&mut self, table_name: ObjectName) -> Result<ExtStatement> {
        if self.parse_cnos_keyword(CnosKeyWord::FIELD) {
            let field_name = self.parser.parse_identifier()?;
            let data_type = self.parse_column_type()?;
            let encoding = if self.peek_cnos_keyword().eq(&Ok(CnosKeyWord::CODEC)) {
                Some(self.parse_codec_type()?)
            } else {
                None
            };
            let column = ColumnOption::new_field(field_name, data_type, encoding);
            Ok(ExtStatement::AlterTable(AlterTable {
                table_name,
                alter_action: AlterTableAction::AddColumn { column },
            }))
        } else if self.parse_cnos_keyword(CnosKeyWord::TAG) {
            let tag_name = self.parser.parse_identifier()?;
            let column = ColumnOption::new_tag(tag_name);
            Ok(ExtStatement::AlterTable(AlterTable {
                table_name,
                alter_action: AlterTableAction::AddColumn { column },
            }))
        } else {
            self.expected("FIELD or TAG", self.parser.peek_token())
        }
    }

    fn parse_alter_table_drop_column(&mut self, table_name: ObjectName) -> Result<ExtStatement> {
        let column_name = self.parser.parse_identifier()?;
        Ok(ExtStatement::AlterTable(AlterTable {
            table_name,
            alter_action: AlterTableAction::DropColumn { column_name },
        }))
    }

    fn parse_alter_table_alter_column(&mut self, table_name: ObjectName) -> Result<ExtStatement> {
        let column_name = self.parser.parse_identifier()?;
        self.parser.expect_keyword(Keyword::SET)?;
        let encoding = self.parse_codec_type()?;
        Ok(ExtStatement::AlterTable(AlterTable {
            table_name,
            alter_action: AlterTableAction::AlterColumnEncoding {
                column_name,
                encoding,
            },
        }))
    }

    fn parse_alter_database(&mut self) -> Result<ExtStatement> {
        let database_name = self.parser.parse_object_name()?;
        self.parser.expect_keyword(Keyword::SET)?;
        let mut options = DatabaseOptions::default();
        if !self.parse_database_option(&mut options)? {
            return parser_err!(format!(
                "expected database option, but found {}",
                self.parser.peek_token()
            ));
        }
        Ok(ExtStatement::AlterDatabase(AlterDatabase {
            name: database_name,
            options,
        }))
    }

    /// Parses the set of
    fn parse_file_compression_type(&mut self) -> Result<String, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => parse_file_compression_type(&w.value),
            unexpected => self.expected("one of GZIP, BZIP2", unexpected),
        }
    }

    fn parse_has_file_compression_type(&mut self) -> bool {
        self.consume_token(&Token::make_keyword("COMPRESSION"))
            & self.consume_token(&Token::make_keyword("TYPE"))
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_csv_has_header(&mut self) -> bool {
        self.consume_token(&Token::make_keyword("WITH"))
            & self.consume_token(&Token::make_keyword("HEADER"))
            & self.consume_token(&Token::make_keyword("ROW"))
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_has_delimiter(&mut self) -> bool {
        self.consume_token(&Token::make_keyword("DELIMITER"))
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_delimiter(&mut self) -> Result<char, ParserError> {
        let token = self.parser.parse_literal_string()?;
        match token.len() {
            1 => Ok(token.chars().next().unwrap()),
            _ => Err(ParserError::TokenizerError(
                "Delimiter must be a single char".to_string(),
            )),
        }
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_has_partition(&mut self) -> bool {
        self.consume_token(&Token::make_keyword("PARTITIONED"))
            & self.consume_token(&Token::make_keyword("BY"))
    }

    /// Parses the set of valid formats
    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_file_format(&mut self) -> Result<String, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => parse_file_type(&w.value),
            unexpected => self.expected("one of PARQUET, NDJSON, or CSV", unexpected),
        }
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_partitions(&mut self) -> Result<Vec<String>, ParserError> {
        let mut partitions: Vec<String> = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok(partitions);
        }

        loop {
            if let Token::Word(_) = self.parser.peek_token() {
                let identifier = self.parser.parse_identifier()?;
                partitions.push(identifier.to_string());
            } else {
                return self.expected("partition name", self.parser.peek_token());
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after partition definition",
                    self.parser.peek_token(),
                );
            }
        }
        Ok(partitions)
    }

    fn parse_create_external_table(&mut self) -> Result<ExtStatement> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;
        let (columns, _) = self.parser.parse_columns()?;
        self.parser
            .expect_keywords(&[Keyword::STORED, Keyword::AS])?;

        // THIS is the main difference: we parse a different file format.
        let file_type = self.parse_file_format()?;

        let has_header = self.parse_csv_has_header();

        let has_delimiter = self.parse_has_delimiter();
        let delimiter = match has_delimiter {
            true => self.parse_delimiter()?,
            false => ',',
        };

        let file_compression_type = if self.parse_has_file_compression_type() {
            self.parse_file_compression_type()?
        } else {
            "".to_string()
        };

        let table_partition_cols = if self.parse_has_partition() {
            self.parse_partitions()?
        } else {
            vec![]
        };

        self.parser.expect_keyword(Keyword::LOCATION)?;
        let location = self.parser.parse_literal_string()?;

        let create = CreateExternalTable {
            name: normalize_sql_object_name(&table_name),
            columns,
            file_type,
            has_header,
            delimiter,
            location,
            table_partition_cols,
            if_not_exists,
            file_compression_type,
        };
        Ok(ExtStatement::CreateExternalTable(create))
    }

    fn parse_create_table(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;
        let columns = self.parse_cnos_columns()?;

        let create = CreateTable {
            name: table_name,
            if_not_exists,
            columns,
        };
        Ok(ExtStatement::CreateTable(create))
    }

    fn parse_database_options(&mut self) -> Result<DatabaseOptions> {
        if self.parser.parse_keyword(Keyword::WITH) {
            let mut options = DatabaseOptions::default();
            loop {
                if !self.parse_database_option(&mut options)? {
                    return Ok(options);
                }
            }
        }
        Ok(DatabaseOptions::default())
    }

    fn parse_database_option(&mut self, options: &mut DatabaseOptions) -> Result<bool> {
        if self.parse_cnos_keyword(CnosKeyWord::TTL) {
            options.ttl = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::SHARD) {
            options.shard_num = Some(self.parse_u64()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::VNODE_DURATION) {
            options.vnode_duration = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::REPLICA) {
            options.replica = Some(self.parse_u64()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::PRECISION) {
            options.precision = Some(self.parse_string_value()?);
        } else {
            return Ok(false);
        }
        Ok(true)
    }

    fn parse_u64(&mut self) -> Result<u64> {
        let num = self.parser.parse_number_value()?.to_string();
        match num.parse::<u64>() {
            Ok(v) => Ok(v),
            Err(_) => {
                parser_err!(format!(
                    "this option should be a unsigned number, but get {}",
                    num
                ))
            }
        }
    }

    fn parse_string_value(&mut self) -> Result<String> {
        let value = self.parser.parse_value()?;
        match value {
            Value::SingleQuotedString(s) => Ok(s),
            _ => parser_err!(format!("expected value, but found : {}", value)),
        }
    }

    fn parse_create_database(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let database_name = self.parser.parse_object_name()?;
        let options = self.parse_database_options()?;
        Ok(ExtStatement::CreateDatabase(CreateDatabase {
            name: database_name,
            if_not_exists,
            options,
        }))
    }

    /// Parse a SQL CREATE statement
    fn parse_create(&mut self) -> Result<ExtStatement> {
        // Currently only supports the creation of external tables
        if self.parser.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table()
        } else if self.parser.parse_keyword(Keyword::TABLE) {
            self.parse_create_table()
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            self.parse_create_database()
        } else {
            self.expected("an object type after CREATE", self.parser.peek_token())
        }
    }

    /// Parse a SQL DROP statement
    fn parse_drop(&mut self) -> Result<ExtStatement> {
        let obj_type = if self.parser.parse_keyword(Keyword::TABLE) {
            ObjectType::Table
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            ObjectType::Database
        } else {
            return self.expected("TABLE,DATABASE after DROP", self.parser.peek_token());
        };
        let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let object_name = self.parser.parse_object_name()?;

        Ok(ExtStatement::Drop(DropObject {
            object_name,
            if_exist,
            obj_type,
        }))
    }

    fn consume_token(&mut self, expected: &Token) -> bool {
        if self.parser.peek_token() == *expected {
            self.parser.next_token();
            true
        } else {
            false
        }
    }

    fn peek_cnos_keyword(&self) -> Result<CnosKeyWord> {
        self.parser.peek_token().to_string().as_str().parse()
    }

    fn parse_cnos_keyword(&mut self, key_word: CnosKeyWord) -> bool {
        if self.peek_cnos_keyword().eq(&Ok(key_word)) {
            self.parser.next_token();
            true
        } else {
            false
        }
    }

    fn parse_cnos_columns(&mut self) -> Result<Vec<ColumnOption>> {
        // -- Parse as is without adding any semantics
        let mut all_columns: Vec<ColumnOption> = vec![];
        let mut field_columns: Vec<ColumnOption> = vec![];

        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok(field_columns);
        }
        loop {
            let name = self.parser.parse_identifier()?;
            let column_type = self.parse_column_type()?;

            let encoding = if self.parser.peek_token().eq(&Token::Comma) {
                None
            } else {
                Some(self.parse_codec_type()?)
            };

            field_columns.push(ColumnOption {
                name,
                is_tag: false,
                data_type: column_type,
                encoding,
            });
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                return parser_err!(format!("table should have TAGS"));
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
            if self.parse_cnos_keyword(CnosKeyWord::TAGS) {
                self.parse_tag_columns(&mut all_columns)?;
                self.parser.expect_token(&Token::RParen)?;
                break;
            }
        }
        // tag1, tag2, ..., field1, field2, ...
        all_columns.append(&mut field_columns);

        Ok(all_columns)
    }

    fn parse_tag_columns(&mut self, columns: &mut Vec<ColumnOption>) -> Result<()> {
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok(());
        }
        loop {
            let name = self.parser.parse_identifier()?;
            columns.push(ColumnOption {
                name,
                is_tag: true,
                data_type: DataType::String,
                encoding: None,
            });
            let is_comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                break;
            }
            if !is_comma {
                return parser_err!(format!(", is expected after column"));
            }
        }
        Ok(())
    }

    fn parse_column_type(&mut self) -> Result<DataType> {
        let token = self.parser.next_token();
        match token {
            Token::Word(w) => match w.keyword {
                Keyword::TIMESTAMP => parser_err!(format!("already have timestamp column")),
                Keyword::BIGINT => {
                    if self.parser.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedBigInt(None))
                    } else {
                        Ok(DataType::BigInt(None))
                    }
                }
                Keyword::DOUBLE => Ok(DataType::Double),
                Keyword::STRING => Ok(DataType::String),
                Keyword::BOOLEAN => Ok(DataType::Boolean),
                _ => parser_err!(format!("{} is not a supported type", w)),
            },
            unexpected => parser_err!(format!("{} is not a type", unexpected)),
        }
    }

    fn parse_codec_encoding(&mut self) -> Result<Encoding, String> {
        self.parser
            .peek_token()
            .to_string()
            .parse()
            .map(|encoding| {
                self.parser.next_token();
                encoding
            })
    }

    fn parse_codec_type(&mut self) -> Result<Encoding> {
        if !self.parse_cnos_keyword(CnosKeyWord::CODEC) {
            return self.expected("CODEC or ','", self.parser.peek_token());
        }

        self.parser.expect_token(&Token::LParen)?;
        if self.parser.peek_token().eq(&Token::RParen) {
            return parser_err!(format!("expect codec encoding type in ()"));
        }
        let encoding = match self.parse_codec_encoding() {
            Ok(encoding) => encoding,
            Err(str) => return parser_err!(format!("{} is not valid encoding", str)),
        };
        self.parser.expect_token(&Token::RParen)?;
        Ok(encoding)
    }
}

fn parse_file_compression_type(s: &str) -> Result<String, ParserError> {
    Ok(s.to_uppercase())
}

/// This is a copy of the equivalent implementation in Datafusion.
fn parse_file_type(s: &str) -> Result<String, ParserError> {
    Ok(s.to_uppercase())
}

/// Normalize a SQL object name
pub fn normalize_sql_object_name(sql_object_name: &ObjectName) -> String {
    sql_object_name
        .0
        .iter()
        .map(normalize_ident)
        .collect::<Vec<String>>()
        .join(".")
}

// Normalize an identifier to a lowercase string unless the identifier is quoted.
pub fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}

// merge "catalog.db" and "db.table" to "catalog.db.table"
// if a.b and b.c => a.b.c
// if a.b and c.d => None
pub fn merge_object_name(db: Option<ObjectName>, table: Option<ObjectName>) -> Option<ObjectName> {
    let (db, table) = match (db, table) {
        (Some(db), Some(table)) => (db, table),
        (Some(db), None) => return Some(db),
        (None, Some(table)) => return Some(table),
        (None, None) => return None,
    };

    let mut db: Vec<Ident> = db.0;
    let mut table: Vec<Ident> = table.0;
    if db.is_empty() {
        return Some(ObjectName(table));
    }
    if table.len() == 1 {
        db.append(&mut table);
        return Some(ObjectName(table));
    }

    if let Some(db_name) = db.last() {
        if let Some(table_db_name) = table.get(table.len() - 2) {
            if !db_name.eq(table_db_name) {
                return None;
            } else {
                let ident = table.remove(table.len() - 1);
                db.push(ident);
                return Some(ObjectName(db));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use datafusion::sql::sqlparser::ast::{Ident, ObjectName, SetExpr, Statement};
    use spi::query::ast::AlterTable;
    use spi::query::ast::{DropObject, ExtStatement};

    use super::*;

    #[test]
    fn test_drop() {
        let sql = "drop table test_tb";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::Drop(DropObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_tb".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type.to_string(), "TABLE".to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "drop table if exists test_tb";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::Drop(DropObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_tb".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
                assert_eq!(obj_type.to_string(), "TABLE".to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "drop database if exists test_db";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::Drop(DropObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
                assert_eq!(obj_type.to_string(), "DATABASE".to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "drop database test_db";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::Drop(DropObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type.to_string(), "DATABASE".to_string());
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_create_table_statement() {
        let sql = "CREATE TABLE IF NOT EXISTS test\
            (column1 BIGINT CODEC(DELTA),\
            column2 STRING CODEC(GZIP),\
            column3 BIGINT UNSIGNED CODEC(NULL),\
            column4 BOOLEAN,\
            column5 DOUBLE CODEC(GORILLA),\
            TAGS(column6, column7))";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::CreateTable(CreateTable {
                name,
                if_not_exists,
                columns,
            }) => {
                assert_eq!(name.to_string(), "test".to_string());
                assert_eq!(if_not_exists.to_string(), "true".to_string());
                assert_eq!(columns.len(), 7);
                assert_eq!(
                    *columns,
                    vec![
                        ColumnOption {
                            name: Ident::from("column6"),
                            is_tag: true,
                            data_type: DataType::String,
                            encoding: None
                        },
                        ColumnOption {
                            name: Ident::from("column7"),
                            is_tag: true,
                            data_type: DataType::String,
                            encoding: None
                        },
                        ColumnOption {
                            name: Ident::from("column1"),
                            is_tag: false,
                            data_type: DataType::BigInt(None),
                            encoding: Some(Encoding::Delta)
                        },
                        ColumnOption {
                            name: Ident::from("column2"),
                            is_tag: false,
                            data_type: DataType::String,
                            encoding: Some(Encoding::Gzip)
                        },
                        ColumnOption {
                            name: Ident::from("column3"),
                            is_tag: false,
                            data_type: DataType::UnsignedBigInt(None),
                            encoding: Some(Encoding::Null)
                        },
                        ColumnOption {
                            name: Ident::from("column4"),
                            is_tag: false,
                            data_type: DataType::Boolean,
                            encoding: None
                        },
                        ColumnOption {
                            name: Ident::from("column5"),
                            is_tag: false,
                            data_type: DataType::Double,
                            encoding: Some(Encoding::Gorilla)
                        }
                    ]
                );
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_insert_values() {
        let sql = "insert public.test(TIME, ta, tb, fa, fb)
                         values
                             (7, '7a', '7b', 7, 7);";

        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match statements[0] {
            ExtStatement::SqlStatement(ref stmt) => match stmt.deref() {
                Statement::Insert {
                    table_name: ref sql_object_name,
                    columns: ref sql_column_names,
                    source: _,
                    ..
                } => {
                    let expect_table_name = &ObjectName(vec![
                        Ident {
                            value: "public".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "test".to_string(),
                            quote_style: None,
                        },
                    ]);

                    let expect_column_names = &vec![
                        Ident {
                            value: "TIME".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "ta".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "tb".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "fa".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "fb".to_string(),
                            quote_style: None,
                        },
                    ];

                    assert_eq!(sql_object_name, expect_table_name);
                    assert_eq!(sql_column_names, expect_column_names);
                }
                _ => panic!("failed"),
            },
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_insert_select() {
        let sql = "insert public.test_insert_subquery(TIME, ta, tb, fa, fb)
                        select column1, column2, column3, column4, column5
                        from
                        (values
                            (7, '7a', '7b', 7, 7));";

        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match statements[0] {
            ExtStatement::SqlStatement(ref stmt) => match stmt.deref() {
                Statement::Insert {
                    table_name: ref sql_object_name,
                    columns: ref sql_column_names,
                    source,
                    ..
                } => {
                    let expect_table_name = &ObjectName(vec![
                        Ident {
                            value: "public".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "test_insert_subquery".to_string(),
                            quote_style: None,
                        },
                    ]);

                    let expect_column_names = &vec![
                        Ident {
                            value: "TIME".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "ta".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "tb".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "fa".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "fb".to_string(),
                            quote_style: None,
                        },
                    ];

                    assert_eq!(sql_object_name, expect_table_name);
                    assert_eq!(sql_column_names, expect_column_names);

                    match source.deref().body.deref() {
                        SetExpr::Select(_) => {}
                        _ => panic!("failed"),
                    }
                }
                _ => panic!("failed"),
            },
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_create_database() {
        let sql = "CREATE DATABASE test WITH TTl '10d' SHARD 5 VNOdE_DURATiON '3d' REPLICA 10 pRECISIOn 'us';";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match statements[0] {
            ExtStatement::CreateDatabase(ref stmt) => {
                let ans = format!("{:?}", stmt);
                println!("{ans}");
                let expectd = r#"CreateDatabase { name: ObjectName([Ident { value: "test", quote_style: None }]), if_not_exists: false, options: DatabaseOptions { ttl: Some("10d"), shard_num: Some(5), vnode_duration: Some("3d"), replica: Some(10), precision: Some("us") } }"#;
                assert_eq!(ans, expectd);
            }
            _ => panic!("impossible"),
        }
    }
    #[test]
    #[should_panic]
    fn test_create_table_without_fields() {
        let sql = "CREATE TABLE test0(TAGS(column6, column7));";
        ExtParser::parse_sql(sql).unwrap();
    }

    #[test]
    fn test_alter_table() {
        let sql = r#"
            ALTER TABLE m ADD TAG t;
            ALTER TABLE m ADD FIELD f BIGINT CODEC(DEFAULT);
            ALTER TABLE m DROP t;
            ALTER TABLE m DROP f;
            ALTER TABLE m ALTER f SET CODEC(DEFAULT);
            ALTER TABLE m ALTER TIME SET CODEC(NULL);
        "#;
        let statement = ExtParser::parse_sql(sql).unwrap();
        let statement: Vec<AlterTable> = statement
            .into_iter()
            .map(|s| match s {
                ExtStatement::AlterTable(s) => s,
                _ => panic!("Expect AlterTable"),
            })
            .collect();
        assert_eq!(
            statement,
            vec![
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::AddColumn {
                        column: ColumnOption {
                            name: Ident::from("t"),
                            is_tag: true,
                            data_type: DataType::String,
                            encoding: None
                        }
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::AddColumn {
                        column: ColumnOption {
                            name: Ident::from("f"),
                            is_tag: false,
                            data_type: DataType::BigInt(None),
                            encoding: Some(Encoding::Default)
                        }
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::DropColumn {
                        column_name: Ident::from("t")
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::DropColumn {
                        column_name: Ident::from("f")
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::AlterColumnEncoding {
                        column_name: Ident::from("f"),
                        encoding: Encoding::Default
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::AlterColumnEncoding {
                        column_name: Ident::from("TIME"),
                        encoding: Encoding::Null
                    }
                }
            ]
        );
    }
}
