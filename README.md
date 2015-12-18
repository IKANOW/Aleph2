[![Build Status](https://travis-ci.org/IKANOW/Aleph2.svg?branch=master)](https://travis-ci.org/IKANOW/Aleph2)  [![Coverage Status](https://coveralls.io/repos/IKANOW/Aleph2/badge.svg)](https://coveralls.io/r/IKANOW/Aleph2)

# Aleph2
The IKANOW v2 meta-database and analytics platform

Aleph2 provides a single, self-consistent, interface and orchestration framework for data-driven applications:
* Plugin different NoSQL/graph/SQL/domain-specific data stores - Aleph2 provides consumers with a technology-independent view of the underlying data (but with the option to access the data stores' underlying capabilities directly at the expense of future compatibility).
* Use JSON to create and schedule ETL ("harvest and enrichment") pipelines using either the provided components (eg Flume/logstash) or by plugging in script or JVM based modules or technologies.
* Use JSON to create, schedule, and interconnect real-time and batch analytics components using either the provided components (eg Storm/Spark/Hadoop; arbitrary JS; efficient JVM implementation of various generic operations such as aggregation and joins) or again by plugging in script or JVM based modules or technologies.
* Overlay a Rules-Based-Access-Control and Single-Sign-On across all operations (generated from a plug-in security provider).

The two key takeaways are:
* The core platform is 100% agnostic to the selection of data stores or analytic or import technologies - anything goes.
* The SDK and REST APIs and UI components provide one friendly and logical interface across all of the diverse technologies in use and hides the implementation details away (but equally exposes them when needed).

