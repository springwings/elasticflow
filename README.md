# ElasticFlow
Elasticflow can be used for the construction of computational flow，support jobs like Inference,DataTrasfer,Data Searcher etc...

![image](https://github.com/springwings/elasticflow/blob/master/architecture.jpg)


# ElasticFlow用来解决什么?

数据中台作为其核心问题之一就是数据逻辑分层，那么我们要如何实现由一个数据层的数据流引导入下一层呢？
ElasticFlow就是为解决该问题而生。


业务中我们或许需要：

	原始日志进入数据库层，
	原始的各种类型数据库数据进入仓库
	原始大数据进入机器学习中间数据层
	机器学习数据进入用户展示层
	通用搜索服务
    数据推断服务 
	...
综上任务都可以通过ElasticFlow，使用其可控的可计算流管道来实现。

# Versions
	version 5.x
	Java>=1.8

# Reader Support
	Hbase 1.2.1
	Kafka 2.1.1
	Mysql
	Oracle

# Writer Support
	ElasticSearch 7.x
	Mysql
	Neo4j 3.5.1
	Vearch
	Solr 5.5.0
	Hbase 1.2.1

# Document
==>>[详细文档参照wiki](https://github.com/springwings/elasticflow/wiki)  

# Changes
5.0 版本对之前版本在架构上全新升级，不再通过Java原生支持深度学习，计算流通过调用外部推断服务rest接口实现数据计算服务。

# EF Plugin develop
   1. pom入包：
   ```xml
      <dependency>
        <groupId>org.elasticflow</groupId>
        <artifactId>elasticflow</artifactId>
        <version>5.0.8</version>
      </dependency>
   ```
   2. testunit代码，例如：
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
       

# develop plan
1 Support external computing Libraries such as so/dll