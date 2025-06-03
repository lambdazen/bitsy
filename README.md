[![Maven Central](https://img.shields.io/maven-central/v/com.lambdazen/bitsy.svg?label=Maven%20Central)](https://search.maven.org/artifact/com.lambdazen/bitsy)

Bitsy is a small, fast, embeddable, durable in-memory graph database that is compatible with Tinkerpop3. 

[The project Wiki](https://github.com/lambdazen/bitsy/wiki) is the official source of documentation. The original version of the database compatible with Tinkerpop2 is available at https://bitbucket.org/lambdazen/bitsy. 

### Git branching strategy

Tags are named release-[version]. Versions start with 3.0. For e.g., release-3.0

Development happens on `master` branch.

## Building it

The project **build time requirement** is [Apache Maven](https://maven.apache.org/), at least version 3.9 and Java 21.
The project **run time requirement is Java 8**.

For quick build (runs no tests nor any other plugin like javadoc)

```
mvn clean install -Dtest=void
```

For UT-only build (will run UTs too)

```
mvn clean install
```

For full build (will run UTs and ITs)

```
mvn clean install -P run-its
```

For publishing setup, see [parent POM](https://github.com/maveniverse/parent).