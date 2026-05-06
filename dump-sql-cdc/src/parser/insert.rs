use base64::{Engine, prelude::BASE64_STANDARD};
use serde_json::json;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Insert,
    Into,
    Values,
    Identifier(Vec<u8>),
    StringLiteral(Vec<u8>),
    Number(Vec<u8>),
    Null,
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Backtick,
    Dot,
    BinaryPrefix,
    EOF,
}

pub struct Lexer {
    input: Vec<u8>,
    pos: usize,
}

impl Lexer {
    pub fn new(input: Vec<u8>) -> Self {
        Lexer { input, pos: 0 }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();

        while self.pos < self.input.len() {
            self.skip_whitespace();

            if self.pos >= self.input.len() {
                break;
            }

            let ch = self.input[self.pos];

            match ch {
                b'(' => {
                    tokens.push(Token::LeftParen);
                    self.pos += 1;
                }
                b')' => {
                    tokens.push(Token::RightParen);
                    self.pos += 1;
                }
                b',' => {
                    tokens.push(Token::Comma);
                    self.pos += 1;
                }
                b';' => {
                    tokens.push(Token::Semicolon);
                    self.pos += 1;
                }
                b'`' => {
                    tokens.push(Token::Backtick);
                    self.pos += 1;
                }
                b'.' => {
                    tokens.push(Token::Dot);
                    self.pos += 1;
                }
                b'\'' => {
                    let s = self.read_string_literal()?;
                    tokens.push(Token::StringLiteral(s));
                }
                b'0'..=b'9' | b'-' | b'+' => {
                    // 数字
                    let n = self.read_number();
                    tokens.push(Token::Number(n));
                }
                b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                    // 标识符或关键字
                    let ident = self.read_identifier();
                    let token = self.match_keyword(&ident);
                    tokens.push(token);
                }
                _ => {
                    return Err(format!(
                        "Unexpected character at position {}: 0x{:02x}",
                        self.pos, ch
                    ));
                }
            }
        }

        tokens.push(Token::EOF);
        Ok(tokens)
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() {
            let ch = self.input[self.pos];
            if ch == b' ' || ch == b'\t' || ch == b'\n' || ch == b'\r' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn read_string_literal(&mut self) -> Result<Vec<u8>, String> {
        self.pos += 1;
        let mut result = Vec::new();

        while self.pos < self.input.len() {
            let ch = self.input[self.pos];

            if ch == b'\'' {
                self.pos += 1;
                if self.pos < self.input.len() && self.input[self.pos] == b'\'' {
                    result.push(b'\'');
                    self.pos += 1;
                } else {
                    break;
                }
            } else if ch == b'\\' {
                self.pos += 1;
                if self.pos < self.input.len() {
                    let escaped = self.input[self.pos];
                    match escaped {
                        b'0' => result.push(0x00),   // \0  NUL character
                        b'\'' => result.push(b'\''), // \'  Single quote
                        b'"' => result.push(b'"'),   // \"  Double quote
                        b'\\' => result.push(b'\\'), // \\  Backslash
                        b'n' => result.push(b'\n'),  // \n  Newline
                        b'r' => result.push(b'\r'),  // \r  Carriage return
                        b't' => result.push(b'\t'),  // \t  Tab
                        b'b' => result.push(0x08),   // \b  Backspace
                        b'Z' => result.push(0x1A),   // \Z  ASCII 26 (Control+Z)
                        b'%' => result.push(b'%'),   // \%  Percent (literal %)
                        b'_' => result.push(b'_'),   // \_  Underscore (literal _)
                        b'x' => {
                            // \x  Hexadecimal escape sequence: \xXX
                            self.pos += 1;
                            if self.pos + 1 < self.input.len() {
                                let h1 = self.input[self.pos] as char;
                                let h2 = self.input[self.pos + 1] as char;
                                if let (Some(d1), Some(d2)) = (h1.to_digit(16), h2.to_digit(16)) {
                                    let byte = ((d1 << 4) | d2) as u8;
                                    result.push(byte);
                                }
                                self.pos += 1;
                            }
                        }
                        _ => result.push(escaped), // 其他情况：直接输出字符（忽略反斜杠）
                    }
                    self.pos += 1;
                }
            } else {
                result.push(ch);
                self.pos += 1;
            }
        }

        Ok(result)
    }

    fn read_number(&mut self) -> Vec<u8> {
        let start = self.pos;
        while self.pos < self.input.len() {
            let ch = self.input[self.pos];
            if ch.is_ascii_digit()
                || ch == b'.'
                || ch == b'-'
                || ch == b'+'
                || ch == b'e'
                || ch == b'E'
            {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_vec()
    }

    fn read_identifier(&mut self) -> Vec<u8> {
        let start = self.pos;
        while self.pos < self.input.len() {
            let ch = self.input[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_vec()
    }

    fn match_keyword(&self, ident: &[u8]) -> Token {
        let upper: Vec<u8> = ident.iter().map(|c| c.to_ascii_uppercase()).collect();

        match upper.as_slice() {
            b"INSERT" => Token::Insert,
            b"INTO" => Token::Into,
            b"VALUES" => Token::Values,
            b"_BINARY" => Token::BinaryPrefix,
            b"NULL" => Token::Null,
            _ => Token::Identifier(ident.to_vec()),
        }
    }
}

#[derive(Debug)]
pub struct InsertStatement {
    pub table_name: String,
    pub rows: Vec<Row>,
}

impl InsertStatement {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn row_values(&self) -> Vec<Vec<serde_json::Value>> {
        return self
            .rows
            .iter()
            .map(|ele| ele.covnert_serde_value())
            .collect::<Vec<Vec<serde_json::Value>>>();
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn covnert_serde_value(&self) -> Vec<serde_json::Value> {
        return self
            .values
            .iter()
            .map(|ele| match ele {
                Value::String(data) => {
                    if data.len() == 1 {
                        return json!(data[0]);
                    }
                    let result = String::from_utf8(data.to_vec()).unwrap_or_else(|_| {
                        let mut result = String::new();

                        BASE64_STANDARD.encode_string(data, &mut result);
                        return result;
                    });
                    return json!(result);
                }
                Value::Number(data) => {
                    let num = data.parse::<i64>().unwrap_or(0);
                    return json!(num);
                }
                Value::Binary(data) => {
                    let mut result = String::new();
                    BASE64_STANDARD.encode_string(data, &mut result);
                    return json!(result);
                }
                Value::Null => {
                    return serde_json::Value::Null;
                }
            })
            .collect::<Vec<serde_json::Value>>();
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    String(Vec<u8>),
    Number(String),
    Null,
    Binary(Vec<u8>),
}

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    pub fn parse(&mut self) -> Result<InsertStatement, String> {
        // 期望: INSERT INTO table_name VALUES (...), (...), ...;

        // 1. 匹配 INSERT
        self.expect_token(Token::Insert)?;

        // 2. 匹配 INTO
        self.expect_token(Token::Into)?;

        // 3. 匹配表名（可能用反引号包裹）
        let table_name = self.parse_table_name()?;

        // 4. 匹配 VALUES
        self.expect_token(Token::Values)?;

        // 5. 解析行数据
        let rows = self.parse_rows()?;

        // 6. 可选的分号
        self.match_token(Token::Semicolon);

        Ok(InsertStatement { table_name, rows })
    }

    fn parse_table_name(&mut self) -> Result<String, String> {
        // 支持两种格式：
        // 1. `table_name`  (反引号包裹)
        // 2. table_name    (直接标识符)

        let has_backtick = self.match_token(Token::Backtick);

        match self.current_token() {
            Some(Token::Identifier(name)) => {
                let table_name = String::from_utf8_lossy(name).to_string();
                self.pos += 1;

                if has_backtick {
                    self.expect_token(Token::Backtick)?;
                }

                Ok(table_name)
            }
            _ => Err(format!(
                "Expected table name, found {:?}",
                self.current_token()
            )),
        }
    }

    fn parse_rows(&mut self) -> Result<Vec<Row>, String> {
        let mut rows = Vec::new();

        // 期望左括号
        self.expect_token(Token::LeftParen)?;

        loop {
            // 解析一行
            let row = self.parse_row()?;
            rows.push(row);

            // 期望右括号
            self.expect_token(Token::RightParen)?;

            // 检查是否还有更多行
            if !self.match_token(Token::Comma) {
                break;
            }

            // 下一行以左括号开始
            self.expect_token(Token::LeftParen)?;
        }

        Ok(rows)
    }

    fn parse_row(&mut self) -> Result<Row, String> {
        let mut values = Vec::new();
        let mut is_binary = false;

        loop {
            // 检查是否有 _binary 前缀
            if self.match_token(Token::BinaryPrefix) {
                is_binary = true;
            }

            match self.current_token() {
                Some(Token::StringLiteral(s)) => {
                    let s = s.clone();
                    self.pos += 1;
                    if is_binary {
                        values.push(Value::Binary(s));
                        is_binary = false;
                    } else {
                        values.push(Value::String(s));
                    }
                }
                Some(Token::Number(n)) => {
                    let n = String::from_utf8_lossy(n).to_string();
                    self.pos += 1;
                    values.push(Value::Number(n));
                }
                Some(Token::Null) => {
                    self.pos += 1;
                    values.push(Value::Null);
                }
                Some(Token::RightParen) => break,
                Some(Token::EOF) => {
                    return Err("Unexpected end of input".to_string());
                }
                Some(_) => {
                    return Err(format!(
                        "Unexpected token in row: {:?}",
                        self.current_token()
                    ));
                }
                None => {
                    return Err("Unexpected end of input".to_string());
                }
            }

            // 检查逗号分隔符
            if !self.match_token(Token::Comma) {
                break;
            }
        }

        Ok(Row { values })
    }

    fn current_token(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn match_token(&mut self, token: Token) -> bool {
        if let Some(t) = self.current_token() {
            if std::mem::discriminant(t) == std::mem::discriminant(&token) {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    fn expect_token(&mut self, token: Token) -> Result<(), String> {
        if let Some(t) = self.current_token() {
            if std::mem::discriminant(t) == std::mem::discriminant(&token) {
                self.pos += 1;
                return Ok(());
            }
        }
        Err(format!(
            "Expected {:?}, found {:?}",
            token,
            self.current_token()
        ))
    }
}

// ============================================================================
// 公共接口
// ============================================================================

/// 解析 INSERT INTO 语句
pub fn parse_insert(sql: &[u8]) -> Result<InsertStatement, String> {
    let mut lexer = Lexer::new(sql.to_vec());
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

/// 转换为 JSON 格式
pub fn to_json(stmt: &InsertStatement) -> Vec<serde_json::Value> {
    stmt.rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            map.insert("table".to_string(), json!(stmt.table_name));

            for (i, value) in row.values.iter().enumerate() {
                let col_name = format!("col{}", i);
                let json_val = match value {
                    Value::String(s) => {
                        // 如果包含空字节或非 ASCII，使用 base64
                        if s.contains(&0) || s.iter().any(|&b| b >= 0x80) {
                            use base64::{Engine as _, engine::general_purpose};
                            json!(general_purpose::STANDARD.encode(s))
                        } else {
                            json!(String::from_utf8_lossy(s))
                        }
                    }
                    Value::Number(n) => json!(n),
                    Value::Null => json!(null),
                    Value::Binary(b) => {
                        use base64::{Engine as _, engine::general_purpose};
                        json!(general_purpose::STANDARD.encode(b))
                    }
                };
                map.insert(col_name, json_val);
            }

            serde_json::Value::Object(map)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_simple() {
        let sql = b"INSERT INTO `test` VALUES (1, 'hello');";
        let mut lexer = Lexer::new(sql.to_vec());
        let tokens = lexer.tokenize().unwrap();
        assert!(matches!(tokens[0], Token::Insert));
        assert!(matches!(tokens[1], Token::Into));
    }

    #[test]
    fn test_parse_simple_insert() {
        let sql = b"INSERT INTO `test_table` VALUES (1, 'hello', 3.14);";
        let stmt = parse_insert(sql).unwrap();
        assert_eq!(stmt.table_name, "test_table");
        assert_eq!(stmt.rows.len(), 1);
        assert_eq!(stmt.rows[0].values.len(), 3);
    }

    #[test]
    fn test_parse_multiple_rows() {
        let sql = b"INSERT INTO `test` VALUES (1,'a'),(2,'b'),(3,'c');";
        let stmt = parse_insert(sql).unwrap();
        assert_eq!(stmt.rows.len(), 3);
    }

    #[test]
    fn test_parse_empty_string() {
        let sql = b"INSERT INTO `test` VALUES ('');";
        let stmt = parse_insert(sql).unwrap();
        assert_eq!(stmt.rows.len(), 1);
        match &stmt.rows[0].values[0] {
            Value::String(s) => assert_eq!(s.len(), 0),
            _ => panic!("Expected empty string"),
        }
    }

    #[test]
    fn test_parse_binary_data() {
        let sql = b"INSERT INTO `test` VALUES (_binary 'hello\\x00world');";
        let stmt = parse_insert(sql).unwrap();
        assert_eq!(stmt.rows.len(), 1);
        match &stmt.rows[0].values[0] {
            Value::Binary(_) => {}
            _ => panic!("Expected binary value"),
        }
    }

    #[test]
    fn test_parse_with_null() {
        let sql = b"INSERT INTO `test` VALUES (1, NULL, 'test');";
        let stmt = parse_insert(sql).unwrap();
        assert_eq!(stmt.rows.len(), 1);
        match &stmt.rows[0].values[1] {
            Value::Null => {}
            _ => panic!("Expected null value"),
        }
    }

    #[test]
    fn test_to_json() {
        let sql = b"INSERT INTO `test` VALUES (1, 'hello', NULL);";
        let stmt = parse_insert(sql).unwrap();
        let json = to_json(&stmt);
        assert_eq!(json.len(), 1);
    }
}
