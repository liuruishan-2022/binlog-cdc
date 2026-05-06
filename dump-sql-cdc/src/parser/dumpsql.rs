use std::{collections::HashMap, fmt::Display, io::BufRead, path::PathBuf, sync::Arc};

use crossbeam_channel::bounded;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sqlparser::{
    ast::{CreateTable, DataType, Statement},
    dialect::MySqlDialect,
    parser::Parser,
};
use tracing::{debug, info, warn};
use walkdir::WalkDir;
use zip::ZipArchive;

use crate::{
    config::cdc::CdcConfig,
    kafka::KafkaSink,
    parser::{
        debezium::DebeziumFormat,
        error::MyError,
        insert,
        sql_parser::{self, SqlType},
    },
};

///
/// 解析mysqldump命令生成的sql文件
///

pub struct MysqlDumpSqlParser {
    table_cache: HashMap<String, Table>,
    sink: Arc<KafkaSink>,
    config: Arc<CdcConfig>,
}

// 定义一个类型别名出来,否则编写起来很麻烦
type SqlLine = Vec<u8>;

impl MysqlDumpSqlParser {
    pub async fn new(config: Arc<CdcConfig>) -> Self {
        let config_arc = config.clone();
        let sink = KafkaSink::create(config_arc).await;
        Self {
            table_cache: HashMap::new(),
            sink: Arc::new(sink),
            config: config,
        }
    }

    pub async fn start(&mut self) {
        for path in self.walk_sql_files() {
            match self.read_zip_file(&path).await {
                Ok(_) => {
                    info!("读取zip文件:{} 成功!", path.display())
                }
                Err(err) => {
                    warn!(error = ?err, "读取zip文件:{} 失败", path.display());
                }
            }
        }
    }

    fn walk_sql_files(&self) -> Vec<PathBuf> {
        WalkDir::new(self.config.cdc().path())
            .into_iter()
            .filter(|ele| ele.is_ok())
            .map(|ele| ele.unwrap())
            .filter(|ele| ele.file_type().is_file())
            .filter(|ele| ele.path().extension().and_then(|ext| ext.to_str()) == Some("zip"))
            .map(|ele| ele.into_path())
            .collect::<Vec<PathBuf>>()
    }

    ///
    /// 经过大致的性能测试,得出如结论:
    /// 1. 当只是读取文件的时候,不做任何的性能解析,单核CPU跑满,大概是: 180w/s的速度,根本不用担心性能问题
    /// 2. 如果增加SQL Parser操作,然后是大概是: 18w/s的速度,CPU跑满了:8-10核
    /// 3. 如果增加到提取数据,然后转成Debezium格式,速度下降到:8-9w/s的速度
    /// 4. 如果增加投递给Kafka的操作，估计速度会下降到到:5-6w/s的速度
    ///
    /// 这个里面的主要性能分支在下面的分析:
    /// 1. 整个步骤是: 读取文件 -> 解析SQL -> 转换成Debezium格式 -> 投递给Kafka
    /// 之后解析SQL是多线程的,使用Rayon来做并行解析,其他都是单线程的,所以性能瓶颈就在后面的操作需要转成多线程才行
    /// 2. 验证了下，即使是一边解析zip文件,一边进行SQL解析,速度依然是:18w/s的速度,对于5600w的数据,大概还是5分钟左右解析完成
    ///
    /// 3. 不要有个点需要优化,那就是,其实只需要我们同步的表进行解析,不需要同步的表不需要进行解析和处理,甚至都不用走到sql解析这个环节
    ///
    /// 目前做了调整和优化性能,发现性能依然可以达到18w/s的速度
    async fn read_zip_file(&mut self, path: &PathBuf) -> Result<(), MyError> {
        info!(
            "开始处理压缩文件:{}",
            path.to_str().expect("获取filenam失败")
        );
        let file = std::fs::File::open(path).unwrap();
        let mut reader = ZipArchive::new(file).unwrap();

        let filenames = reader
            .file_names()
            .map(|ele| ele.to_string())
            .collect::<Vec<String>>();
        for filename in filenames {
            info!("开始处理zip文件内的sql文件:{}", filename);

            // 使用 Path 提取文件名，排除目录
            let path = std::path::Path::new(&filename);
            if path.is_dir() {
                info!("跳过目录:{}", filename);
                continue;
            }

            let actual_filename = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            info!("提取到的文件名:{}", actual_filename);
            if let Some(topic) = self.config.cdc().search_topic(&actual_filename) {
                match self.config.cdc().password() {
                    Some(pwd) => {
                        let mut file_reader = reader
                            .by_name_decrypt(filename.as_str(), pwd.as_bytes())
                            .expect("解析加密zip文件失败");
                        let reader = std::io::BufReader::new(&mut file_reader);
                        self.process_sql_file(reader, &topic).await;
                    }
                    None => {
                        let mut file_reader = reader
                            .by_name(filename.as_str())
                            .expect("解压zip压缩文件失败");
                        let reader = std::io::BufReader::new(&mut file_reader);
                        self.process_sql_file(reader, topic.as_str()).await;
                    }
                }
            } else {
                warn!("文件名字：{} 不在配置文件内,忽略读取!", filename);
            }
        }
        Ok(())
    }

    async fn process_sql_file<R: std::io::Read>(
        &mut self,
        mut reader: std::io::BufReader<R>,
        topic: &str,
    ) {
        let mut line_bytes = Vec::new();
        let mut sql_line: SqlLine = Vec::new();
        let mut sql_buffer: Vec<SqlLine> = Vec::new();

        while reader.read_until(b'\n', &mut line_bytes).unwrap() > 0 {
            if line_bytes.len() <= 2 {
                debug!("空行,丢弃:");
                line_bytes.clear();
                continue;
            }
            if (line_bytes[0] == b'/' && line_bytes[1] == b'*')
                || (line_bytes[0] == b'-' && line_bytes[1] == b'-')
            {
                debug!("注释的行,丢弃:{}", String::from_utf8_lossy(&line_bytes));
                line_bytes.clear();
                continue;
            }
            // 只有有效的sql才会增加到sql_line中
            sql_line.append(&mut line_bytes);

            if sql_line.ends_with(&[b';', b'\n']) {
                debug!(
                    "查找到真实的SQL语句:{}",
                    String::from_utf8_lossy(&sql_line[0..10])
                );
                sql_buffer.push(sql_line);
                if sql_buffer.len() >= self.config.cdc().capacity() as usize {
                    //批量处理
                    self.parallel_parse(sql_buffer, topic).await;
                    sql_buffer = Vec::new();
                }
                sql_line = Vec::new();
            }
        }

        if sql_buffer.len() > 0 {
            self.parallel_parse(sql_buffer, topic).await;
        }
    }

    async fn parallel_parse(&mut self, sql_buffer: Vec<SqlLine>, topic: &str) {
        let mut diversion = sql_buffer
            .into_iter()
            .fold(HashMap::new(), |mut map, sql_line| {
                let sql_type = sql_parser::classify_sql(&sql_line);
                map.entry(sql_type).or_insert_with(Vec::new).push(sql_line);
                map
            });
        if let Some(sql_lines) = diversion.remove(&SqlType::CreateTable) {
            let dialect = MySqlDialect {};
            sql_lines.into_iter().for_each(|line| {
                let line = String::from_utf8(line).unwrap();
                let statement = Parser::new(&dialect)
                    .try_with_sql(&line)
                    .expect("解析Create table语句失败")
                    .parse_statement()
                    .expect("生成SQL的Statement失败");

                if let Statement::CreateTable(event) = statement {
                    self.parse_create_table(&event);
                } else {
                    warn!("判断错误,不是Create Table语句");
                }
            });
        }

        let (tx, rx) = bounded(self.config.cdc().channel_capacity() as usize);

        let producer_threads = self.config.cdc().producer_threads() as usize;
        let sink = self.sink.clone();
        let topic_clone = topic.to_string();

        let mut send_handles = Vec::with_capacity(producer_threads);

        for _ in 0..producer_threads {
            let rx_clone: crossbeam_channel::Receiver<Vec<DebeziumFormat>> = rx.clone();
            let sink = sink.clone();
            let topic = topic_clone.clone();

            let handle = tokio::spawn(async move {
                while let Ok(data) = rx_clone.recv() {
                    sink.send_messages(data, &topic).await;
                }
            });

            send_handles.push(handle);
        }

        drop(rx);

        if let Some(sql_lines) = diversion.remove(&SqlType::Insert) {
            sql_lines
                .into_par_iter()
                .filter_map(|line| self.parse_insert(line))
                .for_each(|ele| {
                    tx.send(ele).expect("发送数据到channel tx失败");
                });
        }

        drop(tx);
        for handle in send_handles {
            handle.await.unwrap();
        }
    }

    fn parse_create_table(&mut self, event: &CreateTable) {
        let name = &event.name.0.get(0).unwrap();
        match name {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                info!("表名:{}", ident.value);
                let columns = self.parse_columns(event);
                let table = Table::new(ident.value.clone(), columns);
                self.table_cache.insert(ident.value.clone(), table);
            }
            _ => {
                warn!("忽略");
            }
        }
    }

    fn parse_columns(&self, event: &CreateTable) -> Vec<Column> {
        event
            .columns
            .iter()
            .enumerate()
            .map(|(index, ele)| Column::new(index, ele.name.value.clone(), ele.data_type.clone()))
            .collect::<Vec<Column>>()
    }

    fn parse_insert(&self, sql: Vec<u8>) -> Option<Vec<DebeziumFormat>> {
        let insert = insert::parse_insert(&sql).expect("解析Insert into 语句失败");
        if self.table_cache.contains_key(insert.table_name()) {
            let table = self.table_cache.get(insert.table_name()).unwrap();
            let values = insert.row_values();
            let datas = DebeziumFormat::build(table, values);
            debug!("解析出来的insert数据个数: {}", datas.len());
            return Some(datas);
        } else {
            warn!(
                "表存在在sql dump的文件中,无法获取对应的字段:{}!",
                insert.table_name()
            );
            return None;
        }
    }
}

pub struct Table {
    name: String,
    columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String, mut columns: Vec<Column>) -> Self {
        columns.sort_by_key(|col| col.index);
        Self { name, columns }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn columns_by_index(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }
}

#[derive(Debug)]
pub struct Column {
    index: usize,
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(index: usize, name: String, data_type: DataType) -> Self {
        Column {
            index,
            name,
            data_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Column {{ index: {}, name: {}, data_type: {:?} }}",
            self.index, self.name, self.data_type
        )
    }
}
