///
/// SQL 类型分流和路由
/// 负责判断 SQL 语句类型并分发到不同的解析器
///
// INSERT 语句的具体解析在 insert.rs 模块中

///
/// SQL 类型枚举
///
#[derive(Debug, PartialEq, Clone, Copy, Eq, Hash)]
pub enum SqlType {
    Insert,
    CreateTable,
    DropTable,
    LockTables,
    UnlockTables,
    Other,
}

///
/// 快速判断 SQL 类型（通过字节匹配，避免 UTF-8 转换）
///
pub fn classify_sql(sql: &[u8]) -> SqlType {
    // 跳过前导空白
    let start = sql
        .iter()
        .position(|&c| !c.is_ascii_whitespace())
        .unwrap_or(0);

    if start >= sql.len() {
        return SqlType::Other;
    }

    // 转换为大写进行比较（不区分大小写）
    let sql_upper: Vec<u8> = sql[start..]
        .iter()
        .map(|c| c.to_ascii_uppercase())
        .collect();

    // INSERT INTO ...
    if sql_upper.starts_with(b"INSERT INTO") {
        return SqlType::Insert;
    }

    // CREATE TABLE ...
    if sql_upper.starts_with(b"CREATE")
        && sql_upper.contains(&b'T')
        && sql_upper.contains(&b'A')
        && sql_upper.contains(&b'B')
        && sql_upper.contains(&b'L')
        && sql_upper.contains(&b'E')
    {
        return SqlType::CreateTable;
    }

    // DROP TABLE ...
    if sql_upper.starts_with(b"DROP")
        && sql_upper.contains(&b'T')
        && sql_upper.contains(&b'A')
        && sql_upper.contains(&b'B')
        && sql_upper.contains(&b'L')
        && sql_upper.contains(&b'E')
    {
        return SqlType::DropTable;
    }

    // LOCK TABLES ...
    if sql_upper.starts_with(b"LOCK")
        && sql_upper.contains(&b'T')
        && sql_upper.contains(&b'A')
        && sql_upper.contains(&b'B')
        && sql_upper.contains(&b'L')
        && sql_upper.contains(&b'E')
    {
        return SqlType::LockTables;
    }

    // UNLOCK TABLES ...
    if sql_upper.starts_with(b"UNLOCK")
        && sql_upper.contains(&b'T')
        && sql_upper.contains(&b'A')
        && sql_upper.contains(&b'B')
        && sql_upper.contains(&b'L')
        && sql_upper.contains(&b'E')
    {
        return SqlType::UnlockTables;
    }

    SqlType::Other
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_insert() {
        let sql = b"INSERT INTO `test` VALUES (1, 'hello');";
        assert_eq!(classify_sql(sql), SqlType::Insert);
    }

    #[test]
    fn test_classify_create_table() {
        let sql = b"CREATE TABLE `test` (id INT);";
        assert_eq!(classify_sql(sql), SqlType::CreateTable);
    }

    #[test]
    fn test_classify_drop_table() {
        let sql = b"DROP TABLE IF EXISTS `test`;";
        assert_eq!(classify_sql(sql), SqlType::DropTable);
    }

    #[test]
    fn test_classify_lock_tables() {
        let sql = b"LOCK TABLES `test` WRITE;";
        assert_eq!(classify_sql(sql), SqlType::LockTables);
    }

    #[test]
    fn test_classify_unlock_tables() {
        let sql = b"UNLOCK TABLES;";
        assert_eq!(classify_sql(sql), SqlType::UnlockTables);
    }

    #[test]
    fn test_classify_case_insensitive() {
        let sql = b"insert into `test` values (1, 'hello');";
        assert_eq!(classify_sql(sql), SqlType::Insert);

        let sql = b"create table `test` (id int);";
        assert_eq!(classify_sql(sql), SqlType::CreateTable);
    }

    #[test]
    fn test_process_sql_with_whitespace() {
        let sql = b"  \n  INSERT INTO `test` VALUES (1, 'hello');";
        assert_eq!(classify_sql(sql), SqlType::Insert);
    }
}
