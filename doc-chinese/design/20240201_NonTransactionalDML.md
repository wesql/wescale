# 非事务DML

在非事务DML tutorial文档中，我们介绍了非事务DML的基本原理和使用方法。在此基础上，这篇文档更多以“开发者的视角”，深入介绍非事务DML的实现细节。该文档会随着非事务DML的优化而更新。

## 相关命令的生命周期
非事务DML主要处理三类请求：subtmit（用户提交非事务DML Job）、show（向用户展示Job的相关信息）和alter（用户对Job进行控制）。
这三类命令经过不同的代码路径后，最终都调用了Job Controller的唯一对外接口HandleRequest。

**submit**

当用户想要提交一个DML Job时，并不需要额外的sql命令，只需要在原先DML基础上增加注释即可。
Wescale vtgate在解析SQL的过程中，若匹配到相应注释，则通过gRPC的方式调用vttablet上的HandleRequest，进而完成Job的提交。

**show**

当用户使用show dml_jobs和show dml_job 'uuid' [details]命令时，词法语法分析器会生成一个ShowDMLJob的ast node。当vtgate识别到该ast node时，会通过gRPC的方式调用vttablet上的HandleRequest，进而获得Job的相关信息。

**alter**

词法语法分析器识别到非事务DML的alter命令后，生成alterDMLJob ast node。与上述两类命令不同，Vitess对于alter命令的处理实现了较好的封装。alter命令通过grpc发送给vttablet后，会根据alter ast node的类型进行分流。针对alterDMLJob ast node，会调用HandleRequest接口，进而实现对Job的控制。

## 支持的 DML
仅支持update、delete两种DML。
仅支持带有where条件的DML。
仅支持对一张非临时表进行修改。
支持在where条件中或update的set子句中使用子查询。
支持一张表拥有多个PK。PK的类型应该在以下范围中：
```go
switch pk.pkType {  
case sqltypes.Float64, sqltypes.Float32, sqltypes.Decimal,  
sqltypes.VarBinary, sqltypes.Blob, sqltypes.Binary, sqltypes.Bit,  
sqltypes.Text,  
sqltypes.Enum, sqltypes.Set, sqltypes.Tuple, sqltypes.Geometry, sqltypes.TypeJSON, sqltypes.Expression,  
sqltypes.HexNum, sqltypes.HexVal, sqltypes.BitNum:  
return true  
}
```

**不支持的DML**

若将以下DML提交为非事务DML，可能会造成预料之外的错误：
update的set子句中，对PK值进行了修改。这将导致某些行在执行的过程中被动态划分到多个batch中，多次被修改。

## 故障恢复
非事务DML的核心组件JobController只在Primary vttablet上运行。当发生Primary切换，或Primary vttablet重启时，能够保证Job的前后状态一致。这主要依赖于以下的设计：
Job主要由三大部分构成：1.保存在内存中的元数据，用于JobController对不同Job的调度。2.持久化在数据库表中的状态数据。3.Runner协程，当Job不处于运行状态时，协程退出。协程运行过程中产生的状态以事务的形式写入数据库表。
当Primary vttablet启动时，会对JobController进行初始化，并从数据库表中读取所有的Job状态，若该Job的状态已经处于submitted之后且不属于结束态，说明在该Job执行完毕前vttablet进程遭遇crash。针对这些Job，构造相应的内存元数据。当所有的元数据恢复完毕后，才开始对Job进行调度。

## PK范围的生成
DML tutorial文档介绍了在表只有一个PK列的情况下，如何为每一个Batch划定PK范围并生成相应的batch sql。
当表中有多列PK时，原理依然相同：将所有DML会影响的行按照PK排序后，每Batch Size行划分成一个Batch，并截取头尾所有PK列的值作为PK范围。
在生成batch sql时，用and和or生成PK范围的相关条件：
例如一个表有两列int类型的PK，分别名为pk1和pk2，某个batch的开始PK值为1(pk1)、5(pk2)，结束值为2(pk1)、7(pk2)，则生成的条件如下：
(pk1 > 1 or pk1 = 1 and pk2 >=5) and (pk1 < 2 or pk1 = 2 and pk2 <= 7)

## batch size
每一个batch的size通过以下公式计算得出：
batchSize = min(threshod_batch_size, user_input_batch_size)
其中user_input_batch_size是用户在提交Job时所指定的batch size，若用户没有指定，则为defaultBatchSize值，该参数可动态配置。
threshold_batch_size通过以下公式计算得出：
default_batch_size = batchSizeThreshold / index_count * ratio
其中batchSizeThreshold参数值可动态配置。index_count为该表的索引数量。ratio为0~1之间的浮点数，可动态配置。

## Show命令内容
当用户执行Show命令后，首先会向job表或batch表进行查询。在查询结果基础上，可以动态增加丰富的字段。
例如：Job表并不存在affected_rows和dealing_batch_id字段。
affected_rows：当用户查询Job表的信息时，会统计该Job所关联的batch表中所有已完成的batch的实际影响行数和，将其作为Job的affected_rows。
dealing_batch_id：当用户查询Job表的信息时，会查询Job所关联的batch表中状态为queued的最小id，将其作为Job的dealing_batch_id。

## Table GC
JobController会定时对处于完成态的Job进行清理，主要包括：该Job在Job表中的条目和该Job关联的batch表。对于前者，直接在Job表中删除该条目。对于后者，将batch表通过table gc的方式进行删除，具体来说：通过将batch表改名，使其进入purge阶段。