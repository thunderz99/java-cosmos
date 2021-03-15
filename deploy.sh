#!/bin/bash

# deploy the jar file to bintray repositories
mvn clean deploy -s settings.xml
