<div align="center">
  <img src="images/logo.jpg" width="160px">
  
  

![GitHub issues](https://img.shields.io/github/issues/springwings/elasticflow)
![GitHub](https://img.shields.io/github/license/springwings/elasticflow)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/springwings/elasticflow)
</div>


# ElasticFlow (伊塔)
ElasticFlow(伊塔)是一个开源弹性流数据交换系统，支持在任意类型数据端之间以及数据与用户之间通过建立可计算弹性流管道，并进行定时定量多渠道方式交换数据服务。项目可应用于数据交换、通用搜索引擎、数据发布服务、数据仓库等项目。

<div align="center">
  <img src="images/flow.jpg" width="520px">
</div>

# ElasticFlow用来解决什么?

- ElasticFlow作为数据交换系统，可以将不同数据源向目标端写入数据，并且还可以支持数据交换过程中的数据计算处理服务。ElasticFlow数据交换过程中有多层级多粒度的控制方式，方便数据的”读端“与”写端“二者进行字段映射工作。除此之外，系统还有一套写端控制机制以支持多任务往同一处写的同步控制机制。
- ElasticFlow可以以分布式系统(Master/Slave)增强其运行性能也可以单节点方式运行，其中分布式部署将支持自动对实例级进行任务负载均衡，以实现大规模的弹性流任务构建。
- 可以解决问题：
    - 不同源之间的数据同步；
    - 对数据进行计算端处理（如接入NLP、CV领域模型进行推理处理）在写入目标端，如视频流业务接入；
    - 构建搜索引擎服务，如增量、全量以及按日期等方式建立索引；
    - 其他系统间需要进行数据交换控制的场景。
    
# Versions
    - version 5.x
    - Java>=11

# 特性
- 支持分布式或者单节点模式运行系统
- 支持分布式任务调度，如系统可自动在集群之间进行任务负载均衡
- 支持多层级并发执行策略，如管道并发、任务并发等多种方式加快数据处理
- 支持任务链式运行，系统提供任务之间的先后顺序依赖运行控制
- 支持任务优先级调度，当资源不足时低优先级任务将自动缩小处理管道
- 支持控制任务的抽象级任务，虚任务可以对多个任务在写端进行同一个事项时的控制
- 支持数据集成与计算，主要是可支持接入外部服务以提供深度学习模型数据推理处理

# Reader Support
    - Hbase 1.2.1
    - Kafka 2.1.1
    - Mysql
    - Oracle

# Computer Support
    - Rest API (POST/JSON)
    
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