Bitsy is a small, fast, embeddable, durable in-memory graph database that is compatible with Tinkerpop3. 

[The project Wiki](https://github.com/lambdazen/bitsy/wiki) is the official source of documentation. The original version of the database compatible with Tinkerpop2 is available at https://bitbucket.org/lambdazen/bitsy. 

### Git branching strategy

Tags are named release-[version]. Versions start with 3.0. For e.g., release-3.0

A note on branch names:

- master: Unstable snapshot of the software on the last-released major version
- dev-[major-version]-branch: Contains newer major versions that are not ready for release. For e.g., dev-3.5-branch
- release-[major-version].x-branch: Contains older major versions which are supported. For e.g., release-3.0-branch

## Building it

Use latest [Apache Maven](https://maven.apache.org/) to build this project. Requires at least version 3.5.0.

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
mvn clean install -Dit
```
