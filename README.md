# ElasticFlow
elasticflow for build computing flow ，support jobs like ML,DataTrasfer,Data Searcher platform,etc

![image](https://github.com/springwings/elasticflow/blob/master/architecture.jpg)


# ElasticFlow用来解决什么?

数据中台作为其核心问题之一就是数据逻辑分层，那么我们要如何实现由一个数据层的数据引导入下一层呢？
ElasticFlow就是为解决该问题而生。



业务中我们或许需要：

	原始日志进入数据库层，
	原始的各种类型数据库数据进入大数据层
	原始大数据进入机器学习中间数据层
	机器学习数据进入用户展示层
	...
综上任务都可以通过ElasticFlow，使用其可控的可计算流管道来实现。

# Versions
$ version 4.x

$ Java>=1.8

$ ES=7.x

==>>[详细文档参照wiki](https://github.com/springwings/elasticflow/wiki)  

# Changes
4.0以后版本对之前版本在架构上全新升级，不仅仅是一个流管道建立以交换数据功能版本，该流将具有可计算的功能。
4.1版本属于持续迭代不断实现版本


# develop plan

1 Improving machine learning algorithm

2 Increase support deeplearning framework,such as tensorflow,dl4j...
