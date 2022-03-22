<div align="center">
  <img src="images/logo.jpg" width="160px">
</div>

# ElasticFlow
Elasticflow是一个分布式异构数据源数据交换处理工具，可用于构建可计算流，数据交换、通用搜索、定时任务等工作...

<div align="center">
  <img src="images/flow.jpg" width="520px">
</div>

# ElasticFlow用来解决什么?

数据中台作为其核心问题之一就是数据逻辑分层，那么我们要如何实现由一个数据层的数据流引导入下一层呢？
ElasticFlow就是为解决该问题而生。


业务中我们或许需要：

	原始日志进入数据库层，
	原始的各种类型数据库数据进入仓库
	原始大数据进入机器学习中间数据层
	机器学习数据进入用户展示层
	通用搜索服务
    定时调度任务 
	...
综上任务都可以通过ElasticFlow，使用其可控的可计算流管道来实现。
ElasticFlow可以以分布式系统(Master/Slave)增强其运行性能也可以单节点方式运行，
其中分布式部署将支持自动对实例级进行任务负载均衡，以实现大规模的弹性流任务构建。

# Versions
    - version 5.x
    - Java>=11

# 特性
    - 支持分布式或者单节点运行
    - 分布式任务调度
    - 多层级并发执行策略
    - 支持DAG任务
    - 支持任务优先级调度
    - 支持控制任务的抽象级任务
    - 支持数据集成与计算

# Reader Support
    - Hbase 1.2.1
    - Kafka 2.1.1
    - Mysql
    - Oracle

# Writer Support
    - ElasticSearch 7.x
    - Mysql
    - Neo4j 3.5.1
    - Vearch
    - Solr 5.5.0
    - Hbase 1.2.1

# Document
==>>[详细文档参照wiki](https://github.com/springwings/elasticflow/wiki)  

# Changes
5.0 版本对之前版本在架构上全新升级，不再通过Java原生支持深度学习，计算流通过调用外部推断服务rest接口实现数据计算服务。

# EF 插件开发
   1. pom入包：
   ```xml
      <dependency>
        <groupId>org.elasticflow</groupId>
        <artifactId>elasticflow</artifactId>
        <version>5.3.6</version>
      </dependency>
   ```
   2. plugin开发测试代码，例如：
   ```java
        @Before
        public void setUp() {
            System.setProperty("config", "file:/work/EF/config");
            System.setProperty("nodeid", "1");
        }
        
        @Test
        public void testPlugin() throws Exception {
            Run.main(null);
            while(true) {
        
            }
        }
   ```
       

# 开发计划
    - 支持外部计算库，如so/dll
    - 优化搜索资源调度。